from __future__ import annotations

import random
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from common.pricing import (
    RiskInputs,
    RiskThresholds,
    compute_premium_multiplier,
    compute_risk_score,
)

BEHAVIOR_PROFILES = {
    "safe": [
        {
            "avg_speed_kmh": 48.0,
            "max_acceleration_ms2": 1.1,
            "harsh_brake_count": 0,
            "steering_stddev": 0.18,
            "lane_departure_count": 0,
        },
        {
            "avg_speed_kmh": 56.0,
            "max_acceleration_ms2": 1.4,
            "harsh_brake_count": 1,
            "steering_stddev": 0.22,
            "lane_departure_count": 0,
        },
        {
            "avg_speed_kmh": 64.0,
            "max_acceleration_ms2": 1.7,
            "harsh_brake_count": 1,
            "steering_stddev": 0.28,
            "lane_departure_count": 0,
        },
    ],
    "aggressive": [
        {
            "avg_speed_kmh": 82.0,
            "max_acceleration_ms2": 2.8,
            "harsh_brake_count": 2,
            "steering_stddev": 0.72,
            "lane_departure_count": 1,
        },
        {
            "avg_speed_kmh": 94.0,
            "max_acceleration_ms2": 3.4,
            "harsh_brake_count": 3,
            "steering_stddev": 0.92,
            "lane_departure_count": 1,
        },
        {
            "avg_speed_kmh": 108.0,
            "max_acceleration_ms2": 3.9,
            "harsh_brake_count": 4,
            "steering_stddev": 1.08,
            "lane_departure_count": 2,
        },
    ],
    "distracted": [
        {
            "avg_speed_kmh": 61.0,
            "max_acceleration_ms2": 1.8,
            "harsh_brake_count": 1,
            "steering_stddev": 1.05,
            "lane_departure_count": 1,
        },
        {
            "avg_speed_kmh": 68.0,
            "max_acceleration_ms2": 2.1,
            "harsh_brake_count": 2,
            "steering_stddev": 1.22,
            "lane_departure_count": 2,
        },
        {
            "avg_speed_kmh": 74.0,
            "max_acceleration_ms2": 2.4,
            "harsh_brake_count": 2,
            "steering_stddev": 1.34,
            "lane_departure_count": 2,
        },
    ],
}

NOISE_STDDEV = {
    "avg_speed_kmh": 1.2,
    "max_acceleration_ms2": 0.12,
    "harsh_brake_count": 0.35,
    "steering_stddev": 0.04,
    "lane_departure_count": 0.25,
}

BOUNDS = {
    "avg_speed_kmh": (0.0, 160.0),
    "max_acceleration_ms2": (0.0, 6.0),
    "harsh_brake_count": (0.0, 8.0),
    "steering_stddev": (0.0, 3.0),
    "lane_departure_count": (0.0, 5.0),
}

RISK_THRESHOLDS = RiskThresholds()


def choose_behavior(rng: random.Random) -> str:
    return rng.choices(
        population=["safe", "aggressive", "distracted"],
        weights=[0.5, 0.3, 0.2],
        k=1,
    )[0]


def sample_profile(behavior_label: str, rng: random.Random) -> dict[str, float]:
    return deepcopy(rng.choice(BEHAVIOR_PROFILES[behavior_label]))


def initialize_vehicle_state(
    demo_session_id: str, vehicle_index: int, segment_steps: int
) -> dict[str, Any]:
    rng = random.Random(f"{demo_session_id}:bootstrap:{vehicle_index}")
    behavior_label = choose_behavior(rng)
    return {
        "vehicle_id": f"demo-veh-{vehicle_index + 1:03d}",
        "driver_id": f"demo-drv-{vehicle_index + 1:03d}",
        "trip_id": f"demo-trip-{demo_session_id[:8]}-{vehicle_index + 1:03d}",
        "behavior_label": behavior_label,
        "profile_a": sample_profile(behavior_label, rng),
        "profile_b": sample_profile(behavior_label, rng),
        "segment_step": 0,
        "segment_steps": segment_steps,
    }


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _interpolate_metric(
    start: float,
    end: float,
    alpha: float,
    noise_stddev: float,
    rng: random.Random,
    bounds: tuple[float, float],
) -> float:
    interpolated = start + alpha * (end - start)
    noisy = interpolated + rng.gauss(0.0, noise_stddev)
    return round(_clamp(noisy, bounds[0], bounds[1]), 4)


def advance_vehicle_state(
    demo_session_id: str,
    vehicle_state: dict[str, Any],
    tick_index: int,
    window_seconds: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    rng = random.Random(
        f"{demo_session_id}:{vehicle_state['vehicle_id']}:{tick_index}"
    )
    current_step = int(vehicle_state["segment_step"])
    segment_steps = max(int(vehicle_state["segment_steps"]), 1)
    alpha = min((current_step + 1) / segment_steps, 1.0)

    values = {
        metric: _interpolate_metric(
            vehicle_state["profile_a"][metric],
            vehicle_state["profile_b"][metric],
            alpha,
            NOISE_STDDEV[metric],
            rng,
            BOUNDS[metric],
        )
        for metric in BOUNDS
    }
    values["harsh_brake_count"] = int(round(values["harsh_brake_count"]))
    values["lane_departure_count"] = int(round(values["lane_departure_count"]))

    risk_score = compute_risk_score(
        RiskInputs(
            avg_speed_kmh=values["avg_speed_kmh"],
            max_acceleration_ms2=values["max_acceleration_ms2"],
            harsh_brake_count=values["harsh_brake_count"],
            steering_stddev=values["steering_stddev"],
            lane_departure_count=values["lane_departure_count"],
        ),
        RISK_THRESHOLDS,
    )
    premium_multiplier = compute_premium_multiplier(risk_score)
    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(seconds=window_seconds)

    payload = {
        "vehicle_id": vehicle_state["vehicle_id"],
        "driver_id": vehicle_state["driver_id"],
        "trip_id": vehicle_state["trip_id"],
        "window_start": window_start.isoformat().replace("+00:00", "Z"),
        "window_end": window_end.isoformat().replace("+00:00", "Z"),
        "mode": "demo",
        "demo_session_id": demo_session_id,
        "avg_speed_kmh": values["avg_speed_kmh"],
        "max_acceleration_ms2": values["max_acceleration_ms2"],
        "harsh_brake_count": values["harsh_brake_count"],
        "steering_stddev": values["steering_stddev"],
        "lane_departure_count": values["lane_departure_count"],
        "risk_score": risk_score,
        "premium_multiplier": premium_multiplier,
        "behavior_class": vehicle_state["behavior_label"],
    }

    next_state = deepcopy(vehicle_state)
    if current_step + 1 >= segment_steps:
        next_behavior = vehicle_state["behavior_label"]
        if rng.random() < 0.25:
            next_behavior = choose_behavior(rng)
        next_state["behavior_label"] = next_behavior
        next_state["profile_a"] = deepcopy(vehicle_state["profile_b"])
        next_state["profile_b"] = sample_profile(next_behavior, rng)
        next_state["segment_step"] = 0
    else:
        next_state["segment_step"] = current_step + 1

    return payload, next_state
