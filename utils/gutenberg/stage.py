#!/usr/bin/env python3
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir

from datasets import load_dataset

dest = storage_dir("gutenberg")
ds = load_dataset("sedthh/gutenberg_english", split="train", streaming=True)
count = 0
with open(dest / "gutenberg_all.jsonl", "w") as f:
    for row in ds:
        json.dump({"id": count, "text": row["TEXT"], "source": row.get("SOURCE",""), "metadata": row.get("METADATA","")}, f)
        f.write("\n")
        count += 1
        if count % 1000 == 0:
            print(f"  {count} books downloaded...")
print(f"Done: {count} books total")
