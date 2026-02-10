import argparse
from enum import Enum
import os
import json
from collections import defaultdict, deque
from util import Tracer


# Strict Two Phase Locking


class LockMode(Enum):
    NONE = 0
    SHARED = 1
    EXCLUSIVE = 2


class LockState:
    def __init__(self) -> None:
        self.lock_mode: LockMode = LockMode.NONE
        self.holders: set[int] = set()
        self.queue: deque[tuple[int, LockMode]] = deque()

    def set_lock_mode(self, mode: LockMode) -> None:
        self.lock_mode = mode

    def add_holder(self, txn_id: int) -> None:
        self.holders.add(txn_id)

    def is_queue_empty(self) -> bool:
        return len(self.queue) == 0

    def add_to_blocked_queue(self, txn_id: int, lock_mode: LockMode):
        self.queue.append((txn_id, lock_mode))

    def get_wait_for_relationships(self) -> set[tuple[int, int]]:
        """I return a set of edges (a,b) s.t. a is waiting for b to relesae lock for this item."""

        edges = set()
        for waiting_txn_id, _ in self.queue:
            for holder_id in self.holders:
                edges.add((waiting_txn_id, holder_id))

        return edges

    # I remove the transaction from the current lock and update the state and next holder(s) accordingly.
    # I return which transactions are unblocked.
    def release_and_grant_next(self, txn_id) -> list[int]:
        """
        Given the current state, can the oldest queued request be granted now?
        """
        if txn_id not in self.holders:
            print("Attempted to release lock for transaction which it didn't hold.")
            return []

        self.holders.remove(txn_id)
        if len(self.holders) == 0:
            self.set_lock_mode(LockMode.NONE)

        unblocked = []

        while not self.is_queue_empty():
            if self.lock_mode == LockMode.EXCLUSIVE:
                break

            # If shared add while next in queue shared.
            elif (
                self.lock_mode == LockMode.SHARED
                and self.queue[0][1] == LockMode.SHARED
            ):
                txn_id, _ = self.queue.popleft()
                self.holders.add(txn_id)
                unblocked.append(txn_id)

            # If none pop the next element and set the mode to the request mode.
            elif self.lock_mode == LockMode.NONE:
                txn_id, mode = self.queue.popleft()
                self.set_lock_mode(mode)
                self.holders.add(txn_id)
                unblocked.append(txn_id)
            elif (
                self.lock_mode == LockMode.SHARED
                and self.queue[0][1] == LockMode.EXCLUSIVE
            ):
                # TODO: If a transaction blocks on a lock request, that operation (and all subsequent ops in the transaction)
                # must wait; when other holders release, a queued X request from the same txn must be granted immediately
                # once it is the sole remaining holder, or the transaction can deadlock waiting on itself.
                # That is, handle upgrade!
                next_txn, _ = self.queue[0]
                if self.holders == {next_txn}:
                    self.queue.popleft()
                    self.set_lock_mode(LockMode.EXCLUSIVE)
                    unblocked.append(next_txn)
                else:
                    break

        return unblocked


class LockManager:
    def __init__(self, tracer: Tracer) -> None:
        self.locks: dict[str, LockState] = {}  # maps an item to its LockState
        self.tracer = tracer

    # I return true is txn acquires shared lock for item false otherwise and add it to queue.
    def acquire_shared_lock(self, txn_id: int, item: str) -> bool:
        """
        I return true if txn id already has a shared lock for item or if it is able to obtain it.
        Othwerwise, I return false and add the transaction to the lock queue.
        Given the current state, can this request be granted immediately?
        """

        # Can I acquire this lock?
        # Yes if:
        #   - No one has lock for this item.
        #   - No other transacton is waiting for the lock.
        #   - Another txn has a shared lock for this item.
        #   - I already have it.
        #    # Already hold this lock? (S or X)

        current_lock_state = self.locks[item] = self.locks.get(item, LockState())

        if txn_id in current_lock_state.holders:
            # Already have X → automatically covers S
            # Already have S → requesting S again, already granted
            return True

        if current_lock_state.lock_mode == LockMode.EXCLUSIVE or (
            not current_lock_state.is_queue_empty()
        ):
            self.locks[item].add_to_blocked_queue(txn_id, LockMode.SHARED)
            return False

        current_lock_state.set_lock_mode(LockMode.SHARED)
        current_lock_state.add_holder(txn_id)
        self.tracer.emit({"event": "LOCK", "item": item, "grant": "S", "to": txn_id})
        return True

    # I return true is txn acquires exclusive lock for item false otherwise and add it to queue.
    def acquire_exclusive_lock(self, txn_id: int, item: str) -> bool:
        """
        I return true if txn id already has an exclusive lock for item or if it is able to obtain it.
        Othwerwise, I return false and add the transaction to the lock queue.
        Given the current state, can this request be granted immediately?
        """
        # Can I acquire this lock?
        # Yes if:
        #   - No one has lock for this item.
        #   - No other transacton is waiting for the lock.
        #   - I already have it

        current_lock_state = self.locks[item] = self.locks.get(item, LockState())

        # Already have lock case.
        if (
            txn_id in current_lock_state.holders
            and current_lock_state.lock_mode == LockMode.EXCLUSIVE
        ):
            return True

        # Upgrade case: have S lock, want X lock.
        if (
            txn_id in current_lock_state.holders
            and current_lock_state.lock_mode == LockMode.SHARED
        ):
            # Cannot upgrade if other locks are waiting for exclusive.
            if not current_lock_state.is_queue_empty():
                self.locks[item].add_to_blocked_queue(txn_id, LockMode.EXCLUSIVE)
                return False

            # Can upgrade only if we're the sole holder
            if len(current_lock_state.holders) == 1:
                current_lock_state.set_lock_mode(LockMode.EXCLUSIVE)
                self.tracer.emit(
                    {"event": "LOCK", "item": item, "grant": "X", "to": txn_id}
                )
                return True
            else:
                # Other transactions also hold S, must wait
                self.locks[item].add_to_blocked_queue(txn_id, LockMode.EXCLUSIVE)
                return False

        # Not already a holder, can we become exclusive holder?
        # That is, status must be none and no other transactions waiting.
        if current_lock_state.lock_mode != LockMode.NONE or (
            not current_lock_state.is_queue_empty()
        ):
            self.locks[item].add_to_blocked_queue(txn_id, LockMode.EXCLUSIVE)
            return False

        # Otherwise, we are the first to acquire this lock.
        current_lock_state.set_lock_mode(LockMode.EXCLUSIVE)
        current_lock_state.add_holder(txn_id)
        self.tracer.emit({"event": "LOCK", "item": item, "grant": "X", "to": txn_id})
        return True

    # I release the lock for any items this txn was a holder for.
    # I return a list of newly unbloked transactions if any.
    def release_locks(self, txn_id: int) -> list[int]:
        all_unblocked = set()

        for _, lock_state in self.locks.items():
            if txn_id in lock_state.holders:
                unblocked = lock_state.release_and_grant_next(txn_id)
                all_unblocked.update(unblocked)

        return list(all_unblocked)

    # I release the lock and wait queues for any items this txn was a holder for.
    def clear_transaction_from_lock_table(self, txn_id: int) -> list[int]:
        unblocked = self.release_locks(txn_id)

        # Note that transaction can only be waiting on one lock at a time (blocked).
        # Remove from queue txn is in any
        for _, lock_state in self.locks.items():
            queue = deque([req for req in lock_state.queue if req[0] != txn_id])

            # txn was filtered out of this queue.
            if len(queue) < len(lock_state.queue):
                lock_state.queue = queue
                return unblocked

        return unblocked

    def get_deadlock_victim_id(self) -> int | None:
        """I return the victim (transaction) id if there is a  lock dependency cycle otherwise, None"""
        adj_list = defaultdict(set)
        for _, lock_state in self.locks.items():
            edges = lock_state.get_wait_for_relationships()
            for waiting_txn, holding_txn in edges:
                adj_list[waiting_txn].add(holding_txn)

        # DFS but return highest txn id in a detected cycle.
        visiting = set()
        visited = set()
        stack = []

        # Does there exist a path that starts and ends on the same vertex in adj list?
        def dfs(txn_id: int) -> int | None:
            visited.add(txn_id)
            visiting.add(txn_id)
            stack.append(txn_id)
            try:
                neighbors = adj_list.get(txn_id, [])  # leaf node if []

                for neighbor in neighbors:
                    if neighbor in visiting:
                        cycle_start_index = stack.index(neighbor)

                        victim_id = max(stack[cycle_start_index:])

                        self.tracer.emit(
                            {
                                "event": "DEADLOCK",
                                "cycle": stack + [victim_id],
                                "victim": victim_id,
                            }
                        )

                        return victim_id

                    if neighbor in visited:
                        continue

                    cycle_max = dfs(neighbor)
                    if cycle_max is not None:
                        return cycle_max

                return None
            finally:
                visiting.remove(txn_id)
                stack.pop()

        for (
            txn_id
        ) in adj_list:  # default dict may add keys when accessing txns not in adj list.
            if txn_id not in visited:
                cycle_max = dfs(txn_id)
                if cycle_max is not None:
                    return cycle_max

        return None


# Transaction Manager Simulator
def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Concurrency Control Simulator - 2PL vs MVCC"
    )

    parser.add_argument(
        "--cc",
        type=str,
        required=True,
        choices=["2pl", "mvcc"],
        help="Concurrency control scheme: 2pl or mvcc",
    )

    parser.add_argument(
        "--schedule",
        type=str,
        required=True,
        help="Path to schedule file (JSONL format)",
    )

    parser.add_argument(
        "--out",
        type=str,
        required=True,
        help="Output directory for trace and final state",
    )

    args = parser.parse_args()

    # Create output directory if it doesn't exist
    os.makedirs(args.out, exist_ok=True)

    return args


def handle_blocked_lock_request(
    txn_id: int,
    event: dict,
    blocked_transactions: set[int],
    pending_events: list[dict],
    lock_manager: LockManager,
) -> int | None:
    blocked_transactions.add(txn_id)  # idempotent...
    pending_events.append(event)

    victim_id = lock_manager.get_deadlock_victim_id()

    return victim_id


def abort_deadlock_victim(
    victim_id: int | None,
    blocked_transactions: set[int],
    aborted_transactions: set[int],
    pending_events: list[dict],
    transaction_buffers: dict[int, dict],
    tracer: Tracer,
    lock_manager: LockManager,
) -> list[dict]:
    if victim_id is None:
        return pending_events

    newly_unblocked = lock_manager.clear_transaction_from_lock_table(victim_id)
    blocked_transactions.difference_update(
        newly_unblocked
    )  # unblock any transactions that acquired locks.
    blocked_transactions.discard(victim_id)
    aborted_transactions.add(victim_id)

    pending_events = [
        pending_event
        for pending_event in pending_events
        if pending_event["t"] != victim_id
    ]

    if victim_id in transaction_buffers:
        del transaction_buffers[victim_id]

    tracer.emit({"event": "ABORT", "t": victim_id})

    return pending_events


# NOTE: Run me anytime the lock state changes (Any other transaction is commited or aborted)!
def process_blocked_events(
    pending_events,
    blocked_transactions,
    aborted_transactions,
    lock_manager,
    transaction_buffers,
    tracer,
    db,
):
    i = 0
    while i < len(pending_events):
        event = pending_events[i]
        txn_id = event["t"]

        if txn_id in aborted_transactions:
            pending_events.pop(i)
            continue

        # If they are still explicitly blocked by something else, move to next
        if txn_id in blocked_transactions:
            i += 1
            continue

        match event:
            case {"op": "COMMIT"}:
                for item, val in transaction_buffers[txn_id].items():
                    db[item] = val

                unblocked = lock_manager.release_locks(txn_id)
                blocked_transactions.difference_update(unblocked)

                tracer.emit(
                    {"event": "OP", "t": txn_id, "op": "COMMIT", "result": "OK"}
                )
                del transaction_buffers[txn_id]
                pending_events.pop(i)

                i = 0
                continue

            case {"op": "ABORT"}:
                unblocked = lock_manager.clear_transaction_from_lock_table(txn_id)
                blocked_transactions.difference_update(unblocked)
                aborted_transactions.add(txn_id)

                tracer.emit({"event": "OP", "t": txn_id, "op": "ABORT", "result": "OK"})
                if txn_id in transaction_buffers:
                    del transaction_buffers[txn_id]
                pending_events.pop(i)

                i = 0
                continue

            case {"op": "R", "item": item}:
                if lock_manager.acquire_shared_lock(txn_id, item):
                    tracer.emit({"event": "UNBLOCK", "t": txn_id, "item": item})
                    val = transaction_buffers[txn_id].get(item, db.get(item))
                    tracer.emit(
                        {
                            "event": "OP",
                            "t": txn_id,
                            "op": "R",
                            "item": item,
                            "result": "OK",
                            "value": val,
                        }
                    )

                    pending_events.pop(i)
                    continue
                else:
                    victim_id = handle_blocked_lock_request(
                        txn_id, event, blocked_transactions, [], lock_manager
                    )
                    if victim_id is not None:
                        # Deadlock! Abort victim, release their locks, and RESTART
                        pending_events = abort_deadlock_victim(
                            victim_id,
                            blocked_transactions,
                            aborted_transactions,
                            pending_events,
                            transaction_buffers,
                            tracer,
                            lock_manager,
                        )
                        i = 0
                        continue

            case {"op": "W", "item": item, "value": value}:
                if lock_manager.acquire_exclusive_lock(txn_id, item):
                    tracer.emit({"event": "UNBLOCK", "t": txn_id, "item": item})
                    transaction_buffers[txn_id][item] = value
                    tracer.emit(
                        {
                            "event": "OP",
                            "t": txn_id,
                            "op": "W",
                            "item": item,
                            "result": "OK",
                            "value": value,
                        }
                    )

                    pending_events.pop(i)
                    continue
                else:
                    victim_id = handle_blocked_lock_request(
                        txn_id, event, blocked_transactions, [], lock_manager
                    )
                    if victim_id is not None:
                        pending_events = abort_deadlock_victim(
                            victim_id,
                            blocked_transactions,
                            aborted_transactions,
                            pending_events,
                            transaction_buffers,
                            tracer,
                            lock_manager,
                        )
                        i = 0
                        continue
        i += 1
    return pending_events


def two_phase_locking_sim(schedule: list[dict], tracer: Tracer, db: dict):
    lock_manager = LockManager(tracer)
    blocked_transactions = set()
    aborted_transactions = set()
    pending_events = []
    transaction_buffers = defaultdict(dict)

    for event in schedule:
        if event["t"] in aborted_transactions:
            continue
        if event["t"] in blocked_transactions:
            pending_events.append(event)
            continue

        match event:
            case {"t": txn_id, "op": "BEGIN"}:
                tracer.emit({"event": "OP", "t": txn_id, "op": "BEGIN", "result": "OK"})

            case {"t": txn_id, "op": "COMMIT"}:
                # Note that if we reach this, the transaction is not currently blocked.
                # Therefore no need to remove from lock queues.

                unblocked_transactions = lock_manager.release_locks(txn_id)

                blocked_transactions.difference_update(unblocked_transactions)

                pending_events = process_blocked_events(
                    pending_events,
                    blocked_transactions,
                    aborted_transactions,
                    lock_manager,
                    transaction_buffers,
                    tracer,
                    db,
                )
                tracer.emit(
                    {"event": "OP", "t": txn_id, "op": "COMMIT", "result": "OK"}
                )

                for item, val in transaction_buffers[txn_id].items():
                    db[item] = val
                del transaction_buffers[txn_id]

            case {"t": txn_id, "op": "ABORT"}:
                # Note that if we reach this, the transaction is not currently blocked.
                # Therefore no need to remove from lock queues.

                unblocked_transactions = lock_manager.release_locks(txn_id)

                blocked_transactions.difference_update(unblocked_transactions)

                pending_events = process_blocked_events(
                    pending_events,
                    blocked_transactions,
                    aborted_transactions,
                    lock_manager,
                    transaction_buffers,
                    tracer,
                    db,
                )
                tracer.emit(
                    {
                        "event": "OP",
                        "t": txn_id,
                        "op": "ABORT",
                        "result": "OK",
                    }
                )
                aborted_transactions.add(txn_id)
                del transaction_buffers[txn_id]

            case {"t": txn_id, "op": "R", "item": item} as event:
                if lock_manager.acquire_shared_lock(txn_id, item):
                    # We have at least shared lock for item, it is safe to read...
                    val = transaction_buffers[txn_id].get(item, db.get(item))
                    tracer.emit(
                        {
                            "event": "OP",
                            "t": txn_id,
                            "op": "R",
                            "item": item,
                            "result": "OK",
                            "value": val,
                        }
                    )
                else:
                    tracer.emit(
                        {
                            "event": "OP",
                            "t": txn_id,
                            "op": "R",
                            "item": item,
                            "result": "BLOCKED",
                            "why": f"waiting for S({item})",
                        }
                    )

                    victim_id = handle_blocked_lock_request(
                        txn_id,
                        event,
                        blocked_transactions,
                        pending_events,
                        lock_manager,
                    )

                    if victim_id is not None:
                        pending_events = abort_deadlock_victim(
                            victim_id,
                            blocked_transactions,
                            aborted_transactions,
                            pending_events,
                            transaction_buffers,
                            tracer,
                            lock_manager,
                        )

                        # Aborting a victim will change the lock state.
                        pending_events = process_blocked_events(
                            pending_events,
                            blocked_transactions,
                            aborted_transactions,
                            lock_manager,
                            transaction_buffers,
                            tracer,
                            db,
                        )

                    continue

            case {"t": txn_id, "op": "W", "item": item, "value": value} as event:
                # Attempt to acquire the exclusive lock.
                if lock_manager.acquire_exclusive_lock(txn_id, item):
                    # We have exclusive lock for item, it is safe to write...
                    transaction_buffers[txn_id][item] = value

                    tracer.emit(
                        {
                            "event": "OP",
                            "t": txn_id,
                            "op": "W",
                            "item": item,
                            "value": value,
                            "result": "OK",
                        }
                    )
                else:
                    tracer.emit(
                        {
                            "event": "OP",
                            "t": txn_id,
                            "op": "W",
                            "item": item,
                            "result": "BLOCKED",
                            "why": f"waiting for X({item})",
                        }
                    )

                    victim_id = handle_blocked_lock_request(
                        txn_id,
                        event,
                        blocked_transactions,
                        pending_events,
                        lock_manager,
                    )

                    if victim_id is not None:
                        pending_events = abort_deadlock_victim(
                            victim_id,
                            blocked_transactions,
                            aborted_transactions,
                            pending_events,
                            transaction_buffers,
                            tracer,
                            lock_manager,
                        )

                        # Aborting a victim will change the lock state.
                        pending_events = process_blocked_events(
                            pending_events,
                            blocked_transactions,
                            aborted_transactions,
                            lock_manager,
                            transaction_buffers,
                            tracer,
                            db,
                        )

                    continue

            case _:
                raise Exception(f"Uknown event format: {event}")

    print(pending_events)


def load_schedule(path):
    with open(path, "r") as file:
        return [json.loads(line) for line in file]


def load_db():
    db_path = os.path.join(os.path.dirname(__file__), "files", "db", "db.json")
    with open(db_path, "r") as db_file:
        return json.load(db_file)


def main():
    args = parse_args()
    schedule = load_schedule(args.schedule)
    db = load_db()
    with Tracer(args.out) as tracer:
        two_phase_locking_sim(schedule, tracer, db)


if __name__ == "__main__":
    main()
