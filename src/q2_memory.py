import json
from collections import defaultdict
from typing import List, Tuple
import emoji


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Returns the 10 most used emojis in the NDJSON dataset.

    Memory-optimized approach:
    - strict streaming (single pass)
    - no intermediate lists
    - minimal temporary objects
    """
    emoji_counts: dict[str, int] = defaultdict(int)

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            text = record.get("content")
            if not text:
                continue

            # iterate directly over generator; no lists allocated
            for match in emoji.emoji_list(text):
                emoji_counts[match["emoji"]] += 1

    # deterministic ordering
    return sorted(
        emoji_counts.items(),
        key=lambda x: (-x[1], x[0])
    )[:10]
