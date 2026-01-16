import json
from collections import defaultdict
from typing import List, Tuple
import emoji


def _get_text(record: dict) -> str | None:
    """
    extract the text from a tweet record.
    In this dataset the correct field is 'content'.
    """
    return record.get("content")


def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Returns the 10 most used emojis in the NDJSON dataset.

    Time-optimized approach:
    - single pass through the file
    - direct counting in dictionary
    - single sorting at the end
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

            text = _get_text(record)
            if not text:
                continue

            for item in emoji.emoji_list(text):
                emoji_counts[item["emoji"]] += 1

    # deterministic top 10 sorting by count desc, emoji asc
    return sorted(
        emoji_counts.items(),
        key=lambda x: (-x[1], x[0])
    )[:10]