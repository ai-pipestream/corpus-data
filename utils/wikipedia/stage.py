#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import storage_dir

from datasets import load_dataset

dest = storage_dir("wikipedia")
ds = load_dataset("wikimedia/wikipedia", "20231101.en", split="train",
                  cache_dir=str(dest / "hf_cache"))
ds.save_to_disk(str(dest / "hf_dataset"))
print(f"Done: {len(ds)} articles")
