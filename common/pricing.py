from __future__ import annotations

from dataclasses import dataclass


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def normalize(value: float, threshold: float) -> float:
    if threshold <= 0:
        raise ValueError("threshold must be greater than 0")
    return clamp(value / threshold, 0.0, 1.0)


@dataclass(frozen=True)
class RiskThresholds:
    speed_kmh: float = 120.0
    acceleration_ms2: float = 4.0
    harsh_brake_count: float = 5.0
    steering_stddev: float = 1.2
    lane_departure_count: float = 3.0


@dataclass(frozen=True)
class RiskInputs:
    avg_speed_kmh: float
    max_acceleration_ms2: float
    harsh_brake_count: int
    steering_stddev: float
    lane_departure_count: int


def compute_risk_score(
    inputs: RiskInputs, thresholds: RiskThresholds | None = None
) -> float:
    thresholds = thresholds or RiskThresholds()
    score = (
        0.30 * normalize(inputs.avg_speed_kmh, thresholds.speed_kmh)
        + 0.20 * normalize(inputs.max_acceleration_ms2, thresholds.acceleration_ms2)
        + 0.25 * normalize(inputs.harsh_brake_count, thresholds.harsh_brake_count)
        + 0.15 * normalize(inputs.steering_stddev, thresholds.steering_stddev)
        + 0.10 * normalize(inputs.lane_departure_count, thresholds.lane_departure_count)
    )
    return round(clamp(score, 0.0, 1.0), 4)


def compute_premium_multiplier(risk_score: float) -> float:
    return round(1.0 + (0.5 * clamp(risk_score, 0.0, 1.0)), 4)
