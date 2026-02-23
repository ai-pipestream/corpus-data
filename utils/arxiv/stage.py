#!/usr/bin/env python3
import sys
import json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir

from datasets import load_dataset

dest = storage_dir("arxiv")
ds = load_dataset("ccdv/arxiv-summarization", split="train", streaming=True)
count = 0
with open(dest / "arxiv_abstracts.jsonl", "w") as f:
    for row in ds:
        json.dump({
            "id": count,
            "abstract": row.get("abstract", ""),
            "article": row.get("article", "")
        }, f)
        f.write("\n")
        count += 1
        if count % 10000 == 0:
            print(f"  {count} papers...")
print(f"Done: {count} papers")
