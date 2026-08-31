"""
Rolling z-score anomaly detector for streaming sensor readings.

Keeps a fixed-size sliding window per sensor and flags a new reading as
anomalous when it falls more than `threshold` standard deviations from the
window's mean. This is intentionally simple (no external state store, no
ML model) so it's cheap to run per-message in a stream processor and easy
to reason about / unit test.
"""

from collections import deque, defaultdict
from dataclasses import dataclass


@dataclass
class AnomalyResult:
    sensor_id: str
    value: float
    mean: float
    std: float
    z_score: float
    is_anomaly: bool


class RollingZScoreDetector:
    def __init__(self, window_size: int = 30, threshold: float = 3.0, min_samples: int = 10):
        if window_size < 2:
            raise ValueError("window_size must be at least 2")
        if min_samples < 2:
            raise ValueError("min_samples must be at least 2")
        if min_samples > window_size:
            raise ValueError("min_samples cannot exceed window_size")

        self.window_size = window_size
        self.threshold = threshold
        self.min_samples = min_samples
        self._windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))

    def update(self, sensor_id: str, value: float) -> AnomalyResult:
        """Feed a new reading for `sensor_id` and return whether it's anomalous.

        The reading being scored is NOT included in its own mean/std
        (baseline is computed from prior readings only), then it's added to
        the window for future calls. Until `min_samples` prior readings
        exist, every reading is reported as not anomalous — you can't judge
        an outlier against a baseline that isn't there yet.
        """
        window = self._windows[sensor_id]

        if len(window) < self.min_samples:
            result = AnomalyResult(sensor_id, value, mean=value, std=0.0, z_score=0.0, is_anomaly=False)
            window.append(value)
            return result

        mean = sum(window) / len(window)
        variance = sum((x - mean) ** 2 for x in window) / len(window)
        std = variance ** 0.5

        if std == 0:
            z_score = 0.0
        else:
            z_score = (value - mean) / std

        is_anomaly = abs(z_score) > self.threshold
        window.append(value)

        return AnomalyResult(sensor_id, value, mean, std, z_score, is_anomaly)

    def reset(self, sensor_id: str | None = None) -> None:
        if sensor_id is None:
            self._windows.clear()
        else:
            self._windows.pop(sensor_id, None)
