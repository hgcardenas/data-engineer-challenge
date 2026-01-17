import json
from collections import defaultdict
from typing import List, Tuple
import emoji


def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Returns the 10 most used emojis in the NDJSON dataset.

    Memory-optimized approach:
    - strict streaming (single pass)
    - avoid per-tweet intermediate lists by iterating emoji tokens via emoji.analyze()
    - minimal temporary objects
    """
    emoji_counts: dict[str, int] = defaultdict(int)

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            text = record.get("content")
            if not text:
                continue

            # emoji.analyze() yields tokens (iterator) instead of building a list per tweet
            for token in emoji.analyze(text):
                # token.chars is the emoji substring (grapheme cluster)
                emoji_counts[token.chars] += 1

    # deterministic ordering
    return sorted(
        emoji_counts.items(),
        key=lambda x: (-x[1], x[0])
    )[:10]
