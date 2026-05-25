from datetime import datetime
from datetime import timezone


def component(label, status, details, **extra):
    return {
        "label": label,
        "status": status,
        "details": details,
        **extra,
    }


def status_rollup(components):
    if any(item.get("status") == "degraded" for item in components.values()):
        return "degraded"
    if any(item.get("status") == "unknown" for item in components.values()):
        return "degraded"
    return "ok"


def normalize_datetime(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def serialize_datetime(value):
    normalized = normalize_datetime(value)
    if normalized is None:
        return None
    return normalized.isoformat()


def age_seconds(value, now=None):
    normalized = normalize_datetime(value)
    if normalized is None:
        return None
    now = normalize_datetime(now or datetime.now(timezone.utc))
    return max(0, int((now - normalized).total_seconds()))


def describe_age(seconds):
    if seconds is None:
        return "no events yet"
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    return f"{hours}h ago"
