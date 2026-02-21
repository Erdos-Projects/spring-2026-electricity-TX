#!/usr/bin/env python3
"""Print checkpoint resume status from state/*.json files."""

from __future__ import annotations

import json
import os
from pathlib import Path


def main() -> None:
    state_dir = Path(os.getenv("STATE_DIR", "state"))
    files = sorted(state_dir.glob("*.json"))

    print(f"State directory: {state_dir}")
    if not files:
        print("No dataset checkpoint files found.")
        return

    print("dataset\tstatus\tnext_doc_index\tlast_listed_page\tlast_completed_doc_id\tupdated_at")
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"{path.stem}\tERROR\t-\t-\t-\t{exc}")
            continue
        status = payload.get("status", "-")
        next_doc_index = payload.get("next_doc_index", 0)
        last_listed_page = payload.get("last_listed_page", 0)
        last_doc_id = payload.get("last_completed_doc_id") or "-"
        updated_at = payload.get("updated_at", "-")
        print(
            f"{path.stem}\t{status}\t{next_doc_index}\t"
            f"{last_listed_page}\t{last_doc_id}\t{updated_at}"
        )


if __name__ == "__main__":
    main()
