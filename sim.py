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


def mvcc_sim(schedule: list[dict[str, list]], tracer: Tracer, db: dict):
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

    transaction_buffer = defaultdict(lambda: defaultdict(list))

    for event in schedule:
        match event:
            case {"t": txn_id, "op": "BEGIN"}:
                pass

            case {"t": txn_id, "op": "COMMIT"}:
                pass

            case {"t": txn_id, "op": "ABORT"}:
                pass

            case {"t": txn_id, "op": "R", "item": item} as event:
                pass

            case {"t": txn_id, "op": "W", "item": item, "value": value} as event:
                pass

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
            db = {
                item: [{"val": val, "begin": 0, "end": float("inf"), "txn": None}]
                for item, val in db.items()
            }
            mvcc_sim(schedule, tracer, db)
        else:
            print(f"Unknown concurrency model: {args.cc}")

    db_out_path = os.path.join(args.out, "db.json")
    with open(db_out_path, "w") as out_db_file:
        json.dump(db, out_db_file)


if __name__ == "__main__":
    main()
