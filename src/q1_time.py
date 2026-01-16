import datetime
import json
import os
from typing import List, Tuple


def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    

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

    date_counts: dict[datetime.date, int] = {}
    date_user_counts: dict[datetime.date, dict[str, int]] = {}

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

            d, user = _extract_date_and_user(obj)
            if d is None or user is None:
                continue

            date_counts[d] = date_counts.get(d, 0) + 1
            per_user = date_user_counts.get(d)
            if per_user is None:
                per_user = {}
                date_user_counts[d] = per_user
            per_user[user] = per_user.get(user, 0) + 1

    if not date_counts:
        return []

    top_dates = sorted(date_counts.items(), key=lambda x: (-x[1], x[0]))[:10]

    result: List[Tuple[datetime.date, str]] = []
    for d, _ in top_dates:
        per_user = date_user_counts.get(d, {})
        if not per_user:
            result.append((d, ""))
            continue

        max_count = max(per_user.values())
        top_users = [u for u, c in per_user.items() if c == max_count]
        result.append((d, min(top_users)))

    return result
