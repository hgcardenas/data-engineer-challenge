import json
import re
from collections import defaultdict
from typing import List, Tuple

# Twitter usernames: max 15 chars, letters/digits/underscore
_MENTION_RE = re.compile(r"@([A-Za-z0-9_]{1,15})")


def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Returns the top 10 most mentioned usernames in the dataset.

    Time-optimized approach:
    - single pass streaming NDJSON
    - primary source: 'mentionedUsers' (structured, avoids regex)
    - fallback: regex over 'content' only when 'mentionedUsers' is missing/empty
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
            if mentioned:
                # mentionedUsers is a list of dicts with 'username'
                for u in mentioned:
                    if not isinstance(u, dict):
                        continue
                    username = u.get("username")
                    if username:
                        counts[username] += 1
                continue

            # Fallback to parsing content only if structured mentions are absent
            text = record.get("content")
            if not text:
                continue

            for m in _MENTION_RE.finditer(text):
                counts[m.group(1)] += 1

    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:10]
