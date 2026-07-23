#!/usr/bin/env python3
"""Count duplicate log records in an exported log JSON file.

A record is considered a duplicate of another when all three of these
fields match exactly: timestamp, message, request id. If any of the
three differs, the records are distinct.

The request id is read from whichever of these keys is present (in
priority order): aws.lambda_request_id, requestId, faas.execution.

Usage:
    python3 scripts/count_duplicate_logs.py exportedLogRecords.JSON
    python3 scripts/count_duplicate_logs.py exportedLogRecords.JSON --filter retry-trigger
    python3 scripts/count_duplicate_logs.py exportedLogRecords.JSON --show 10
"""

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path


REQUEST_ID_KEYS = ("aws.lambda_request_id", "requestId", "faas.execution")
INNER_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z")


def extract_request_id(record):
    for key in REQUEST_ID_KEYS:
        value = record.get(key)
        if value:
            return value
    return None


def extract_inner_timestamp(message):
    if not message:
        return None
    match = INNER_TIMESTAMP_RE.search(message)
    return match.group(0) if match else None


def build_key(record, timestamp_source):
    message = record.get("message")
    if timestamp_source == "inner":
        ts = extract_inner_timestamp(message)
    else:
        ts = record.get("timestamp")
    return (ts, message, extract_request_id(record))


def count_duplicates(records, message_filter=None, timestamp_source="outer"):
    counts = Counter()
    skipped_missing_fields = 0
    for record in records:
        message = record.get("message")
        if message_filter and (message is None or message_filter not in message):
            continue
        key = build_key(record, timestamp_source)
        if key[0] is None or key[1] is None or key[2] is None:
            skipped_missing_fields += 1
            continue
        counts[key] += 1

    duplicate_groups = {key: count for key, count in counts.items() if count > 1}
    extra_copies = sum(count - 1 for count in duplicate_groups.values())
    return counts, duplicate_groups, extra_copies, skipped_missing_fields


def format_preview(message, width=80):
    preview = (message or "").replace("\n", " ").replace("\t", " ").strip()
    if len(preview) > width:
        preview = preview[: width - 3] + "..."
    return preview


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("path", type=Path, help="Path to exportedLogRecords.JSON")
    parser.add_argument(
        "--filter",
        default=None,
        help="Only consider records whose message contains this substring (e.g. 'retry-trigger').",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=0,
        help="Show up to N duplicate groups (largest first) with their counts.",
    )
    parser.add_argument(
        "--timestamp-source",
        choices=("outer", "inner"),
        default="outer",
        help=(
            "Which timestamp to key on: 'outer' uses the record's top-level 'timestamp' "
            "field; 'inner' extracts the ISO-8601 timestamp embedded in the message body "
            "(the one written by the Lambda runtime). Default: outer."
        ),
    )
    args = parser.parse_args(argv)

    if not args.path.is_file():
        print(f"error: {args.path} is not a file", file=sys.stderr)
        return 2

    with args.path.open("r", encoding="utf-8") as fh:
        records = json.load(fh)

    if not isinstance(records, list):
        print("error: expected the file to contain a JSON array of records", file=sys.stderr)
        return 2

    counts, duplicate_groups, extra_copies, skipped = count_duplicates(
        records, args.filter, args.timestamp_source
    )

    total_input = len(records)
    considered = sum(counts.values())

    print(f"records in file:         {total_input}")
    print(f"timestamp source:        {args.timestamp_source}")
    if args.filter:
        print(f"filter applied:          message contains {args.filter!r}")
    print(f"records considered:      {considered}")
    if skipped:
        print(f"skipped (missing keys):  {skipped}")
    print(f"unique (ts, msg, rid):   {len(counts)}")
    print(f"duplicate groups:        {len(duplicate_groups)}")
    print(f"duplicate records:       {extra_copies}  (copies beyond the first in each group)")

    if args.show and duplicate_groups:
        print("\nTop duplicate groups:")
        top = sorted(duplicate_groups.items(), key=lambda kv: kv[1], reverse=True)[: args.show]
        for (ts, msg, rid), count in top:
            print(f"  x{count}  ts={ts}  rid={rid}  msg={format_preview(msg)!r}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
