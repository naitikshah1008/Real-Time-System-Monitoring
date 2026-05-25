METRIC_DETAILS = {
    "cpu": {
        "label": "CPU",
        "unit": "%",
        "impact": "The host may feel slow or stop responding if compute demand keeps rising.",
        "recommendation": "Check recent workload changes, hot processes, and autoscaling capacity.",
    },
    "mem": {
        "label": "Memory",
        "unit": "%",
        "impact": "The host may start swapping or terminating processes if memory pressure continues.",
        "recommendation": "Look for leaks, large allocations, and services with growing resident memory.",
    },
    "disk": {
        "label": "Disk",
        "unit": "%",
        "impact": "Writes can fail and databases can stall when disk pressure reaches critical levels.",
        "recommendation": "Free space, rotate logs, or increase the disk allocation before writes fail.",
    },
    "net_in": {
        "label": "Network In",
        "unit": " KB/s",
        "impact": "Inbound traffic is unusually high and may affect downstream services.",
        "recommendation": "Inspect recent clients, load balancer traffic, and unexpected data transfers.",
    },
    "net_out": {
        "label": "Network Out",
        "unit": " KB/s",
        "impact": "Outbound traffic is unusually high and may indicate fan-out or data egress.",
        "recommendation": "Check response sizes, retry loops, and external dependency traffic.",
    },
}

SCENARIO_LABELS = {
    "cpu_spike": "CPU Spike",
    "memory_leak": "Memory Leak",
    "disk_pressure": "Disk Pressure",
    "network_burst": "Network Burst",
    "normal": "Normal",
    "unknown": "Unknown",
}


def _as_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_value(value, unit):
    number = _as_float(value)
    if unit == "%":
        return f"{number:.1f}%"
    if unit:
        return f"{number:.1f}{unit}"
    return f"{number:.1f}"


def format_scenario(scenario):
    if not scenario:
        return "Unknown"
    parts = str(scenario).split("+")
    return " + ".join(SCENARIO_LABELS.get(part, part.replace("_", " ").title()) for part in parts)


def explain_anomaly(row):
    metric = row.get("metric", "unknown")
    details = METRIC_DETAILS.get(
        metric,
        {
            "label": metric.replace("_", " ").title(),
            "unit": "",
            "impact": "This signal moved away from its learned baseline.",
            "recommendation": "Compare this event with recent deployments, traffic, and host activity.",
        },
    )
    value = _as_float(row.get("value"))
    baseline = _as_float(row.get("baseline"))
    score = _as_float(row.get("score"))
    severity = row.get("severity") or "warning"
    direction = "above" if value >= baseline else "below"
    unit = details["unit"]
    scenario = format_scenario(row.get("scenario"))

    return {
        **row,
        "metric_label": details["label"],
        "scenario_label": scenario,
        "severity_label": severity.title(),
        "headline": f"{severity.title()} {details['label']} anomaly on {row.get('host', 'unknown host')}",
        "explanation": (
            f"{details['label']} is {direction} its learned baseline "
            f"({_format_value(value, unit)} vs {_format_value(baseline, unit)}). "
            f"Anomaly score: {score:.1f}. Scenario: {scenario}."
        ),
        "impact": details["impact"],
        "recommendation": details["recommendation"],
    }


def enrich_anomalies(rows):
    return [explain_anomaly(row) for row in rows]


def summarize_current_metric(row):
    if not row:
        return {
            "headline": "Waiting for telemetry",
            "details": "No advanced metric rows have been stored yet.",
            "scenario_label": "Unknown",
        }

    scenario = format_scenario(row.get("scenario"))
    if row.get("scenario") == "normal":
        details = "The latest event is normal simulated host telemetry."
    else:
        details = f"The latest event reflects an active simulated {scenario} scenario."

    return {
        "headline": f"{row.get('host', 'Unknown host')} is reporting {scenario}",
        "details": details,
        "scenario_label": scenario,
    }
