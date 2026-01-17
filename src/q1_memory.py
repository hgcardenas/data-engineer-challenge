import datetime
import json
import os
from typing import List, Tuple


def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    if not isinstance(file_path, str) or not file_path.strip():
        raise ValueError("file_path must be a non-empty string.")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    def _extract_date_and_user(obj):
        dt_val = obj.get("date") or obj.get("created_at") or obj.get("datetime") or obj.get("timestamp")
        if not dt_val:
            return None, None

        if not isinstance(dt_val, str):
            dt_val = str(dt_val)

        ds = dt_val[:10]
        try:
            d = datetime.date(int(ds[0:4]), int(ds[5:7]), int(ds[8:10]))
        except Exception:
            try:
                d = datetime.datetime.fromisoformat(dt_val.replace("Z", "+00:00")).date()
            except Exception:
                return None, None

        user = None
        u = obj.get("user")
        if isinstance(u, dict):
            user = u.get("username") or u.get("screen_name") or u.get("name")
        if not user:
            user = obj.get("username") or obj.get("screen_name") or obj.get("user_name")

        if not user:
            return d, None

        return d, str(user)

    # Pass 1: count tweets per date (low memory)
    date_counts: dict[datetime.date, int] = {}

    with open(file_path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        checked_format = False
        for line in f:
            if not checked_format:
                s = line.lstrip()
                if s.startswith("["):
                    raise ValueError(
                        "Input appears to be a JSON array, not NDJSON (one JSON object per line). "
                        "This solution expects NDJSON for streaming."
                    )
                checked_format = True

            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception:
                continue

            if not isinstance(obj, dict):
                continue

            d, _user = _extract_date_and_user(obj)
            if d is None:
                continue

            date_counts[d] = date_counts.get(d, 0) + 1

    if not date_counts:
        return []

    top_dates = sorted(date_counts.items(), key=lambda x: (-x[1], x[0]))[:10]
    top_date_set = {d for d, _ in top_dates}

    # Pass 2: count users only for the top 10 dates (bounded memory)
    # Structure: {date: {user: count}}
    per_date_user_counts: dict[datetime.date, dict[str, int]] = {d: {} for d in top_date_set}

    with open(file_path, "rt", encoding="utf-8", errors="replace", newline="") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception:
                continue

            if not isinstance(obj, dict):
                continue

            d, user = _extract_date_and_user(obj)
            if d is None or user is None:
                continue

            if d not in top_date_set:
                continue

            day_map = per_date_user_counts[d]
            day_map[user] = day_map.get(user, 0) + 1

    result: List[Tuple[datetime.date, str]] = []
    for d, _cnt in top_dates:
        day_map = per_date_user_counts.get(d, {})
        if not day_map:
            result.append((d, ""))
            continue

        max_count = max(day_map.values())
        candidates = [u for u, c in day_map.items() if c == max_count]
        result.append((d, min(candidates)))

    return result
