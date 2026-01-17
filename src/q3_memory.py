import json
from collections import defaultdict
from typing import List, Tuple


def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    Returns the top 10 most mentioned usernames in the dataset.

    Memory-optimized approach:
    - strict streaming (single pass)
    - use only the structured 'mentionedUsers' field (canonical signal)
    - avoid regex/text parsing to minimize temporary allocations and false positives
    - deterministic ordering: count desc, username asc
    """
    counts: dict[str, int] = defaultdict(int)

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            mentioned = record.get("mentionedUsers")
            if not mentioned:
                continue

            for u in mentioned:
                if not isinstance(u, dict):
                    continue
                username = u.get("username")
                if username:
                    counts[username] += 1

    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:10]
