from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from common.models import BehaviorLabel, SensorEvent

from .dataset_loader import TelemetryRow


@dataclass(frozen=True)
class SensorConfig:
    freq_hz: float
    unit: str
    noise_stddev: float
    min_value: float
    max_value: float

    def __post_init__(self) -> None:
        if self.freq_hz <= 0:
            raise ValueError("Sensor frequency must be greater than 0.")
        if self.min_value > self.max_value:
            raise ValueError(
                "Sensor min_value must be less than or equal to max_value."
            )


@dataclass(frozen=True)
class FleetConfig:
    vehicle_count: int
    interpolation_window_seconds: float
    behavior_switch_probability: float
    sensor_configs: dict[str, SensorConfig]

    def __post_init__(self) -> None:
        if self.vehicle_count <= 0:
            raise ValueError("vehicle_count must be greater than 0.")
        if self.interpolation_window_seconds <= 0:
            raise ValueError("interpolation_window_seconds must be greater than 0.")
        if not 0 <= self.behavior_switch_probability <= 1:
            raise ValueError("behavior_switch_probability must be between 0 and 1.")
        if not self.sensor_configs:
            raise ValueError("At least one sensor configuration is required.")


@dataclass
class VehicleState:
    vehicle_id: str
    driver_id: str
    trip_id: str
    behavior_label: BehaviorLabel
    row_a: TelemetryRow
    row_b: TelemetryRow
    segment_start: datetime
    next_sample_at: dict[str, datetime]
    sequence_id: int = 0


FIELD_MAP = {
    "speed": "speed",
    "acceleration": "acceleration",
    "brake_intensity": "brake_intensity",
    "steering_variability": "steering_variability",
    "lane_deviation": "lane_deviation",
}


class FleetSimulator:
    def __init__(
        self,
        config: FleetConfig,
        grouped_rows: dict[BehaviorLabel, list[TelemetryRow]],
        seed: int = 42,
    ) -> None:
        self.config = config
        self.grouped_rows = grouped_rows
        self.random = random.Random(seed)
        self.window = timedelta(seconds=config.interpolation_window_seconds)
        self.vehicles: list[VehicleState] = []
        self._bootstrap()

    def _bootstrap(self) -> None:
        now = datetime.now(timezone.utc)
        for index in range(self.config.vehicle_count):
            behavior = self._choose_behavior()
            row_a = self._sample_row(behavior)
            row_b = self._sample_row(behavior)
            next_sample_at = {
                sensor_name: now for sensor_name in self.config.sensor_configs.keys()
            }
            self.vehicles.append(
                VehicleState(
                    vehicle_id=f"veh-{index + 1:03d}",
                    driver_id=f"drv-{index + 1:03d}",
                    trip_id=f"trip-{now:%Y%m%d}-{index + 1:03d}",
                    behavior_label=behavior,
                    row_a=row_a,
                    row_b=row_b,
                    segment_start=now,
                    next_sample_at=next_sample_at,
                )
            )

    def _choose_behavior(self) -> BehaviorLabel:
        return self.random.choices(
            population=["safe", "aggressive", "distracted"],
            weights=[0.5, 0.3, 0.2],
            k=1,
        )[0]

    def _sample_row(self, behavior: BehaviorLabel) -> TelemetryRow:
        return self.random.choice(self.grouped_rows[behavior])

    def _roll_segment(self, vehicle: VehicleState) -> None:
        if self.random.random() < self.config.behavior_switch_probability:
            vehicle.behavior_label = self._choose_behavior()
        vehicle.row_a = vehicle.row_b
        vehicle.row_b = self._sample_row(vehicle.behavior_label)
        vehicle.segment_start = vehicle.segment_start + self.window

    def _interpolated_value(
        self, vehicle: VehicleState, sensor_name: str, sample_time: datetime
    ) -> float:
        elapsed = (sample_time - vehicle.segment_start).total_seconds()
        alpha = min(max(elapsed / self.window.total_seconds(), 0.0), 1.0)
        field_name = FIELD_MAP[sensor_name]
        start_value = getattr(vehicle.row_a, field_name)
        end_value = getattr(vehicle.row_b, field_name)
        config = self.config.sensor_configs[sensor_name]
        interpolated = start_value + alpha * (end_value - start_value)
        noisy = interpolated + self.random.gauss(0.0, config.noise_stddev)
        return round(max(config.min_value, min(config.max_value, noisy)), 4)

    def collect_due_events(self, now: datetime | None = None) -> list[SensorEvent]:
        now = now or datetime.now(timezone.utc)
        events: list[SensorEvent] = []

        for vehicle in self.vehicles:
            while now - vehicle.segment_start >= self.window:
                self._roll_segment(vehicle)

            for sensor_name, sensor_config in self.config.sensor_configs.items():
                interval = timedelta(seconds=1 / sensor_config.freq_hz)
                while vehicle.next_sample_at[sensor_name] <= now:
                    sample_time = vehicle.next_sample_at[sensor_name]
                    vehicle.sequence_id += 1
                    events.append(
                        SensorEvent(
                            vehicle_id=vehicle.vehicle_id,
                            driver_id=vehicle.driver_id,
                            trip_id=vehicle.trip_id,
                            timestamp=sample_time,
                            sequence_id=vehicle.sequence_id,
                            sensor_type=sensor_name,
                            value=self._interpolated_value(
                                vehicle, sensor_name, sample_time
                            ),
                            unit=sensor_config.unit,
                            behavior_label=vehicle.behavior_label,
                        )
                    )
                    vehicle.next_sample_at[sensor_name] = sample_time + interval

        return events
