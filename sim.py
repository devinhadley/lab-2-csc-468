import argparse
import os
import json
from util import Tracer
from two_part_locking import two_phase_locking_sim
from collections import defaultdict


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


# item -> [(value, start_time, end_time)] # float is to allow for infinity...
DBWithVersionHistory = dict[str, list[tuple[int, int, float]]]


def get_latest_value_committed_before_snapshot(
    db: DBWithVersionHistory, start_timestamp: int, item: str
) -> int:
    for val, begin, end in reversed(db[item]):
        if begin <= start_timestamp < end:
            return val

    raise Exception("No versioned value for " + item)


def get_latest_value_start_time(db: DBWithVersionHistory, item: str) -> int:
    values = db[item]

    if len(values) == 0:
        raise Exception("No versioned value for " + item)

    return values[-1][1]


def write_new_value(
    db: DBWithVersionHistory,
    clock_time: int,
    item: str,
    new_value: int,
):
    values = db[item]
    last_val, last_begin, _ = values[-1]

    previous_val = (last_val, last_begin, clock_time)
    values[-1] = previous_val

    values.append((new_value, clock_time, float("inf")))


def cleanup_transaction(
    transaction_buffers: dict[int, dict],
    transaction_starts: dict[int, int],
    txn_id: int,
) -> None:
    del transaction_buffers[txn_id]
    del transaction_starts[txn_id]


def mvcc_sim(schedule: list[dict], tracer: Tracer, db: DBWithVersionHistory):
    # txn -> item -> list of (value, creation, expiration)

    # Each transaction reads from a snapshot.
    # Snapshot is defined when the transaction starts.
    # So esentially, for the entirety of the transaction
    # it only reads data which was matching the state of when it started.

    # Reads only see:
    #   - Only versions commited before the snapshot.
    #   - A consistent view of the database.
    #   - Esentially a frozen picture of the database.
    #   - V.commit_ts ≤ T.snapshot_ts

    # Transaction cant commit if:
    #   - It writes to an item who was already written to during its lifetime.

    transaction_buffers: dict[
        int, dict
    ] = {}  # txn -> item -> value (prevent dirty reads...)
    transaction_starts: dict[int, int] = {}  # txn -> start_time
    clock = 0  # every commit the clock increments by one.

    for event in schedule:
        match event:
            case {"t": txn_id, "op": "BEGIN"}:
                transaction_buffers[txn_id] = {}
                transaction_starts[txn_id] = clock
                tracer.emit({"event": "OP", "t": txn_id, "op": "BEGIN", "result": "OK"})

            case {"t": txn_id, "op": "COMMIT"}:
                # LatestVersion.begin_ts > MyTransaction.start_ts ⟹ ABORT

                # All or nothing!
                should_abort = False
                for item in transaction_buffers[txn_id]:
                    latest_version_begin_timestamp = get_latest_value_start_time(
                        db, item
                    )

                    # We're overwriting a state we never even saw... ABORT!
                    if latest_version_begin_timestamp > transaction_starts[txn_id]:
                        tracer.emit({"event": "ABORT", "t": txn_id})
                        cleanup_transaction(
                            transaction_buffers, transaction_starts, txn_id
                        )
                        should_abort = True
                        break

                if should_abort:
                    continue

                for item, value in transaction_buffers[txn_id].items():
                    write_new_value(db, clock, item, value)

                tracer.emit(
                    {"event": "OP", "t": txn_id, "op": "COMMIT", "result": "OK"}
                )

                # Cleanup
                cleanup_transaction(transaction_buffers, transaction_starts, txn_id)

            case {"t": txn_id, "op": "ABORT"}:
                tracer.emit({"event": "OP", "t": txn_id, "op": "ABORT", "result": "OK"})
                cleanup_transaction(transaction_buffers, transaction_starts, txn_id)

            case {"t": txn_id, "op": "R", "item": item} as event:
                val = transaction_buffers[txn_id].get(item)

                # 2. If it's not in the buffer (None), fall back to the snapshot logic
                if val is None:
                    # Note: Your helper returns a tuple (val, begin, end)
                    val = get_latest_value_committed_before_snapshot(
                        # We only need the value for the read result.
                        db,
                        transaction_starts[txn_id],
                        item,
                    )

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

            case {"t": txn_id, "op": "W", "item": item, "value": val} as event:
                transaction_buffers[txn_id][item] = val
                tracer.emit(
                    {
                        "event": "OP",
                        "t": txn_id,
                        "op": "W",
                        "item": item,
                        "result": "OK",
                        "value": val,
                    }
                )

            case _:
                raise Exception(f"Uknown event format: {event}")
    pass


def load_schedule(path):
    with open(path, "r") as file:
        return [json.loads(line) for line in file]


def load_db():
    db_path = os.path.join(os.path.dirname(__file__), "files", "db", "db.json")
    with open(db_path, "r") as db_file:
        return json.load(db_file)


def main():
    args = parse_args()
    os.makedirs(args.out, exist_ok=True)

    db = load_db()
    schedule = load_schedule(args.schedule)
    with Tracer(args.out) as tracer:
        if args.cc == "2pl":
            two_phase_locking_sim(schedule, tracer, db)
        elif args.cc == "mvcc":
            # transform the DB into a verisioned DB for MVCC.
            db = {item: [(val, 0, float("inf"))] for item, val in db.items()}
            mvcc_sim(schedule, tracer, db)
        else:
            print(f"Unknown concurrency model: {args.cc}")

    if args.cc == "mvcc":
        # Extract only the value from the latest version of each item
        db = {item: versions[-1][0] for item, versions in db.items()}

    db_out_path = os.path.join(args.out, "db.json")
    with open(db_out_path, "w") as out_db_file:
        json.dump(db, out_db_file)


if __name__ == "__main__":
    main()
