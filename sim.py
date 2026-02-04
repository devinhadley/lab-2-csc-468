import collections
import argparse
from enum import Enum
import os
import json
import logging


# Strict Two Phase Locking


from enum import Enum
from collections import deque


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

    # I remove the transaction from the current lock and update the state and next holder(s) accordingly.
    # I return which transactions are unblocked.
    def release_and_grant_next(self, txn_id) -> list[int]:
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
                self.lock_mode = mode
                self.holders.add(txn_id)
                unblocked.append(txn_id)

        return unblocked


class LockManager:
    def __init__(self) -> None:
        self.locks: dict[str, LockState] = {}  # maps an item to its LockState

    # I return true is txn acquires shared lock for item false otherwise and add it to queue.
    def acquire_shared_lock(self, txn_id: int, item: str) -> bool:
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
            self.locks[item].queue.append((txn_id, LockMode.SHARED))
            return False

        current_lock_state.set_lock_mode(LockMode.SHARED)
        current_lock_state.add_holder(txn_id)
        return True

    # I return true is txn acquires exclusive lock for item false otherwise and add it to queue.
    def acquire_exclusive_lock(self, txn_id: int, item: str) -> bool:
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
                current_lock_state.queue.append((txn_id, LockMode.EXCLUSIVE))
                return False

            # Can upgrade only if we're the sole holder
            if len(current_lock_state.holders) == 1:
                current_lock_state.set_lock_mode(LockMode.EXCLUSIVE)
                return True
            else:
                # Other transactions also hold S, must wait
                current_lock_state.queue.append((txn_id, LockMode.EXCLUSIVE))
                return False

        # Not already a holder, can we become exclusive holder?
        # That is, status must be none and no other transactions waiting.
        if current_lock_state.lock_mode != LockMode.NONE or (
            not current_lock_state.is_queue_empty()
        ):
            self.locks[item].queue.append((txn_id, LockMode.EXCLUSIVE))
            return False

        # Otherwise, we are the first to acquire this lock.
        current_lock_state.set_lock_mode(LockMode.EXCLUSIVE)
        current_lock_state.add_holder(txn_id)
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


def main():
    args = parse_args()

    lock_manager = LockManager()
    blocked_transactions = set()

    # Load the entire schedule in memory...
    schedule = []
    with open(args.schedule, "r") as f:
        schedule = [json.loads(line) for line in f]

    for event in schedule:
        match event:
            case {"t": txn_id, "op": "BEGIN"}:
                pass
            case {"t": txn_id, "op": "COMMIT"}:
                # Release all locks with txn_id...
                pass
            case {"t": txn_id, "op": "ABORT"}:
                # Release all locks with txn_id...
                lock_manager.release_locks(txn_id)

                blocked_transactions.remove(69)
                # TODO: Run any previous events that were blocked by this lock!
                # That is, for all the transactions that now have the lock, rerun all possible events until blocked.
                #

                pass

            case {"t": txn_id, "op": "R", "item": item}:
                if txn_id in blocked_transactions:
                    continue

                if not lock_manager.acquire_shared_lock(txn_id, item):
                    blocked_transactions.add(txn_id)
                    continue

                pass
            case {"t": txn_id, "op": "W", "item": item}:
                if txn_id in blocked_transactions:
                    continue

                # Acquire exlusive lock.
                if not lock_manager.acquire_exclusive_lock(txn_id, item):
                    blocked_transactions.add(txn_id)
                    continue
            case _:
                raise Exception(f"Uknown event format: {event}")


if __name__ == "__main__":
    main()
