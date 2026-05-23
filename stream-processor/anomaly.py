import math


TRACKED_METRICS = ("cpu", "mem", "disk", "net_in", "net_out")


class EWMADetector:
    def __init__(self, alpha=0.2, threshold=3.0):
        self.alpha = alpha
        self.threshold = threshold
        self.state = {}

    def detect(self, event):
        anomalies = []
        for metric in TRACKED_METRICS:
            value = float(event[metric])
            key = (event["host"], metric)
            previous = self.state.get(key)

            if previous is None:
                self.state[key] = (value, value * value)
                continue

            ewma, ewmsq = previous
            baseline = ewma
            variance = max(ewmsq - ewma * ewma, 0.0)
            std = math.sqrt(variance)
            if std > 0:
                score = abs(value - baseline) / std
            else:
                score = 0.0 if value == baseline else abs(value - baseline) / max(abs(baseline) * 0.05, 1.0)

            ewma_new = self.alpha * value + (1 - self.alpha) * ewma
            ewmsq_new = self.alpha * (value * value) + (1 - self.alpha) * ewmsq
            self.state[key] = (ewma_new, ewmsq_new)

            if score >= self.threshold:
                anomalies.append(
                    {
                        "event_id": f"{event['event_id']}-{metric}",
                        "host": event["host"],
                        "ts": event["ts"],
                        "metric": metric,
                        "value": round(value, 4),
                        "baseline": round(baseline, 4),
                        "std": round(std, 4),
                        "score": round(score, 4),
                        "severity": "critical" if score >= 5 else "warning",
                        "scenario": event.get("scenario", "unknown"),
                    }
                )

        return anomalies
