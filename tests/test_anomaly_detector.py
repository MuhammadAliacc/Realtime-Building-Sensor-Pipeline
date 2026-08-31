import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from processor.anomaly_detector import RollingZScoreDetector


def test_no_anomaly_before_min_samples():
    d = RollingZScoreDetector(window_size=10, threshold=3.0, min_samples=5)
    for v in [20.0, 21.0, 19.5, 1000.0]:  # even an extreme value shouldn't trip during warmup
        result = d.update("s1", v)
        assert result.is_anomaly is False


def test_flags_clear_outlier_after_warmup():
    d = RollingZScoreDetector(window_size=20, threshold=3.0, min_samples=5)
    baseline = [20.0, 20.2, 19.8, 20.1, 19.9, 20.0, 20.3, 19.7]
    for v in baseline:
        result = d.update("s1", v)
        assert result.is_anomaly is False

    spike = d.update("s1", 200.0)
    assert spike.is_anomaly is True
    assert spike.z_score > 3.0


def test_stable_readings_never_flagged():
    d = RollingZScoreDetector(window_size=15, threshold=3.0, min_samples=5)
    for v in [50.0] * 30:  # zero variance baseline
        result = d.update("s1", v)
        assert result.is_anomaly is False


def test_sensors_are_independent():
    d = RollingZScoreDetector(window_size=10, threshold=3.0, min_samples=5)
    for v in [20.0] * 8:
        d.update("s1", v)

    # s2 has never been seen, so it's still warming up and shouldn't flag
    result = d.update("s2", 9999.0)
    assert result.is_anomaly is False


def test_window_size_caps_memory():
    d = RollingZScoreDetector(window_size=5, threshold=3.0, min_samples=3)
    for v in range(100):
        d.update("s1", float(v))
    assert len(d._windows["s1"]) == 5


@pytest.mark.parametrize("window_size,min_samples", [(1, 1), (5, 6), (0, 1)])
def test_invalid_config_raises(window_size, min_samples):
    with pytest.raises(ValueError):
        RollingZScoreDetector(window_size=window_size, min_samples=min_samples)


def test_reset_clears_state():
    d = RollingZScoreDetector(window_size=10, threshold=3.0, min_samples=3)
    for v in [20.0] * 5:
        d.update("s1", v)
    assert len(d._windows["s1"]) == 5

    d.reset("s1")
    assert "s1" not in d._windows
