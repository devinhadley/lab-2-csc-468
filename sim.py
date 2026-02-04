import collections
import argparse
from enum import Enum
import os
import json


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

    def is_queue_not_empty(self) -> bool:
        return len(self.queue) == 0


class LockManager:
    def __init__(self) -> None:
        self.locks: dict[str, LockState] = {}  # maps an item to its LockState

    def acquire_shared_lock(self, txn_id: int, item: str) -> bool:
        # Can I acquire this lock?
        # Yes if:
        #   - No one has lock for this item.
        #   - No other transacton is waiting for the lock.
        #   - Another txn has a shared lock for this item.
        current_lock_state = self.locks[item] = self.locks.get(item, LockState())
        if (
            current_lock_state.lock_mode == LockMode.EXCLUSIVE
            or current_lock_state.is_queue_not_empty()
        ):
            self.locks[item].queue.append((txn_id, LockMode.SHARED))
            return False

        current_lock_state.set_lock_mode(LockMode.SHARED)
        current_lock_state.add_holder(txn_id)
        return True

    def acquire_exclusive_lock(self, txn_id: int, item: str) -> bool:
        # Can I acquire this lock?
        # Yes if:
        #   - No one has lock for this item.
        #   - No other transacton is waiting for the lock.
        #   - Another txn has a shared lock for this item.
        return True

    def release_locks(self, txn_id: int):
        pass


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
