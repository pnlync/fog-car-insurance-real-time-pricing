from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from common.models import BehaviorLabel

ALIASES = {
    "behavior_label": ["behavior_label", "label", "behavior", "driving_behavior"],
    "speed": ["speed", "speed_kmh", "vehicle_speed"],
    "acceleration": ["acceleration", "acceleration_ms2", "longitudinal_acceleration"],
    "brake_intensity": ["brake_intensity", "braking_intensity", "brake"],
    "steering_variability": [
        "steering_variability",
        "steering_variation",
        "steering",
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

        columns = {
            key: _match_column(reader.fieldnames, aliases)
            for key, aliases in ALIASES.items()
        }
        grouped: dict[BehaviorLabel, list[TelemetryRow]] = {
            "safe": [],
            "aggressive": [],
            "distracted": [],
        }

        for raw_row in reader:
            behavior = _normalize_behavior(raw_row[columns["behavior_label"]])
            grouped[behavior].append(
                TelemetryRow(
                    behavior_label=behavior,
                    speed=float(raw_row[columns["speed"]]),
                    acceleration=float(raw_row[columns["acceleration"]]),
                    brake_intensity=float(raw_row[columns["brake_intensity"]]),
                    steering_variability=float(
                        raw_row[columns["steering_variability"]]
                    ),
                    lane_deviation=float(raw_row[columns["lane_deviation"]]),
                )
            )

    for behavior, rows in grouped.items():
        if not rows:
            raise ValueError(f"No rows found for behavior={behavior!r} in dataset.")
    return grouped
