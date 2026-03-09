from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path

from common.models import BehaviorLabel

ALIASES = {
    "behavior_label": ["behavior_label", "label", "behavior", "driving_behavior"],
    "speed": ["speed", "speed_kmh", "speed_kmph", "vehicle_speed"],
    "acceleration": [
        "acceleration",
        "acceleration_ms2",
        "longitudinal_acceleration",
        "accel_x",
    ],
    "brake_intensity": [
        "brake_intensity",
        "braking_intensity",
        "brake",
        "brake_pressure",
    ],
    "steering_variability": [
        "steering_variability",
        "steering_variation",
        "steering",
        "steering_angle",
    ],
    "lane_deviation": ["lane_deviation", "lane_deviation_m", "lane_offset"],
}


@dataclass(frozen=True)
class TelemetryRow:
    behavior_label: BehaviorLabel
    speed: float
    acceleration: float
    brake_intensity: float
    steering_variability: float
    lane_deviation: float


def _match_column(fieldnames: list[str], aliases: list[str]) -> str:
    normalized = {name.strip().lower(): name for name in fieldnames}
    for alias in aliases:
        if alias.lower() in normalized:
            return normalized[alias.lower()]
    raise KeyError(f"Could not find column for aliases: {aliases}")


def _normalize_behavior(value: str) -> BehaviorLabel:
    lowered = value.strip().lower()
    mapping = {
        "safe": "safe",
        "aggressive": "aggressive",
        "distracted": "distracted",
    }
    if lowered not in mapping:
        raise ValueError(
            "Unsupported behavior label. Expected safe/aggressive/distracted, "
            f"got: {value!r}"
        )
    return mapping[lowered]  # type: ignore[return-value]


def _safe_max(values: list[float]) -> float:
    return max(values) if values else 1.0


def _safe_max_abs(values: list[float]) -> float:
    return max((abs(value) for value in values), default=1.0)


def _derive_acceleration(
    raw_row: dict[str, str], columns: dict[str, str], matched_fieldnames: dict[str, str]
) -> float:
    acceleration = float(raw_row[columns["acceleration"]])
    accel_x_column = matched_fieldnames.get("accel_x")
    accel_y_column = matched_fieldnames.get("accel_y")
    if accel_x_column and accel_y_column:
        acceleration = math.copysign(
            math.hypot(
                float(raw_row[accel_x_column]),
                float(raw_row[accel_y_column]),
            ),
            float(raw_row[accel_x_column]),
        )
    return acceleration


def _scale_brake_intensity(
    raw_value: float, brake_column: str, brake_max: float
) -> float:
    if brake_column.lower() == "brake_pressure":
        return min(max(raw_value / brake_max, 0.0), 1.0)
    return raw_value


def _scale_steering_variability(
    raw_value: float, steering_column: str, steering_abs_max: float
) -> float:
    if steering_column.lower() == "steering_angle":
        return min((abs(raw_value) / steering_abs_max) * 3.0, 3.0)
    return raw_value


def _coerce_lane_deviation(raw_value: float) -> float:
    return abs(raw_value)


def load_behavior_rows(path: str | Path) -> dict[BehaviorLabel, list[TelemetryRow]]:
    dataset_path = Path(path)
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Dataset file not found at {dataset_path}. "
            "Download the Kaggle dataset or use the sample CSV in data/."
        )

    with dataset_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("Dataset file has no header row.")

        rows = list(reader)
        columns = {
            key: _match_column(reader.fieldnames, aliases)
            for key, aliases in ALIASES.items()
        }
        matched_fieldnames = {name.strip().lower(): name for name in reader.fieldnames}
        brake_values = [float(raw_row[columns["brake_intensity"]]) for raw_row in rows]
        steering_values = [
            float(raw_row[columns["steering_variability"]]) for raw_row in rows
        ]
        brake_max = _safe_max(brake_values)
        steering_abs_max = _safe_max_abs(steering_values)
        grouped: dict[BehaviorLabel, list[TelemetryRow]] = {
            "safe": [],
            "aggressive": [],
            "distracted": [],
        }

        for raw_row in rows:
            behavior = _normalize_behavior(raw_row[columns["behavior_label"]])
            acceleration = _derive_acceleration(raw_row, columns, matched_fieldnames)
            brake_intensity = _scale_brake_intensity(
                float(raw_row[columns["brake_intensity"]]),
                columns["brake_intensity"],
                brake_max,
            )
            steering_variability = _scale_steering_variability(
                float(raw_row[columns["steering_variability"]]),
                columns["steering_variability"],
                steering_abs_max,
            )
            lane_deviation = _coerce_lane_deviation(
                float(raw_row[columns["lane_deviation"]])
            )
            grouped[behavior].append(
                TelemetryRow(
                    behavior_label=behavior,
                    speed=float(raw_row[columns["speed"]]),
                    acceleration=acceleration,
                    brake_intensity=brake_intensity,
                    steering_variability=steering_variability,
                    lane_deviation=lane_deviation,
                )
            )

    for behavior, rows in grouped.items():
        if not rows:
            raise ValueError(f"No rows found for behavior={behavior!r} in dataset.")
    return grouped
