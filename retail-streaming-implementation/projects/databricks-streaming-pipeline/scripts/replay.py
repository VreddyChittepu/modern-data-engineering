"""Replay operator-corrected NDJSON with new IDs and links to original IDs.
Export only investigated quarantine rows, correct them, then pass the corrected file.
For CDC corrections increase source sequence after source-owner approval.
"""
import argparse
import json
import uuid
from pathlib import Path
from generate import publish

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--corrected-file", required=True)
    p.add_argument("--output", required=True)
    a = p.parse_args()
    records = []
    for line in Path(a.corrected_file).read_text().splitlines():
        record = json.loads(line)
        old_id = record["event_id"]
        record["event_id"] = "replay-" + uuid.uuid4().hex
        record.setdefault("metadata", {})["replay_of"] = old_id
        records.append(record)
    publish(a.output, records)
