import argparse
import os
import json
from util import Tracer
from two_part_locking import two_phase_locking_sim
from mvcc import mvcc_sim


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
