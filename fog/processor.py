from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import mean, pstdev

from common.models import AggregatedWindow, BehaviorLabel, SensorBatch, SensorEvent
from common.pricing import (
    RiskInputs,
    RiskThresholds,
    compute_premium_multiplier,
    compute_risk_score,
)


def floor_window(timestamp: datetime, window_seconds: int) -> datetime:
    epoch_seconds = int(timestamp.timestamp())
    floored = epoch_seconds - (epoch_seconds % window_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


@dataclass(frozen=True)
class ProcessorConfig:
    window_seconds: int
    harsh_brake_threshold: float
    lane_departure_threshold: float
    thresholds: RiskThresholds

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be greater than 0.")


@dataclass
class WindowBucket:
    vehicle_id: str
    driver_id: str
    trip_id: str
    window_start: datetime
    events_by_type: dict[str, list[float]] = field(
        default_factory=lambda: defaultdict(list)
    )
    behaviors: Counter[BehaviorLabel] = field(default_factory=Counter)

    def add_event(self, event: SensorEvent) -> None:
        self.events_by_type[event.sensor_type].append(event.value)
        self.behaviors[event.behavior_label] += 1


class FogProcessor:
    def __init__(self, config: ProcessorConfig) -> None:
        self.config = config
        self.buckets: dict[tuple[str, str, datetime], WindowBucket] = {}

    def consume_batch(self, batch: SensorBatch) -> None:
        for event in batch.events:
            window_start = floor_window(event.timestamp, self.config.window_seconds)
            key = (event.vehicle_id, event.trip_id, window_start)
            bucket = self.buckets.setdefault(
                key,
                WindowBucket(
                    vehicle_id=event.vehicle_id,
                    driver_id=event.driver_id,
                    trip_id=event.trip_id,
                    window_start=window_start,
                ),
            )
            bucket.add_event(event)

    def flush_ready(self, now: datetime | None = None) -> list[AggregatedWindow]:
        now = now or datetime.now(timezone.utc)
        ready: list[AggregatedWindow] = []

        for key, bucket in list(self.buckets.items()):
            window_end = bucket.window_start + timedelta(
                seconds=self.config.window_seconds
            )
            if window_end <= now:
                ready.append(self._build_window(bucket, window_end))
                del self.buckets[key]

        return ready

    def _build_window(
        self, bucket: WindowBucket, window_end: datetime
    ) -> AggregatedWindow:
        speeds = bucket.events_by_type.get("speed", [0.0])
        accelerations = [
            abs(value) for value in bucket.events_by_type.get("acceleration", [0.0])
        ]
        brakes = bucket.events_by_type.get("brake_intensity", [])
        steering = bucket.events_by_type.get("steering_variability", [0.0])
        lane = bucket.events_by_type.get("lane_deviation", [])

        avg_speed = mean(speeds)
        max_acceleration = max(accelerations, default=0.0)
        harsh_brake_count = sum(
            1 for value in brakes if value >= self.config.harsh_brake_threshold
        )
        steering_stddev = pstdev(steering) if len(steering) > 1 else 0.0
        lane_departure_count = sum(
            1 for value in lane if value >= self.config.lane_departure_threshold
        )
        risk_inputs = RiskInputs(
            avg_speed_kmh=avg_speed,
            max_acceleration_ms2=max_acceleration,
            harsh_brake_count=harsh_brake_count,
            steering_stddev=steering_stddev,
            lane_departure_count=lane_departure_count,
        )
        risk_score = compute_risk_score(risk_inputs, self.config.thresholds)
        premium_multiplier = compute_premium_multiplier(risk_score)
        behavior_class = (
            bucket.behaviors.most_common(1)[0][0] if bucket.behaviors else "safe"
        )

        return AggregatedWindow(
            vehicle_id=bucket.vehicle_id,
            driver_id=bucket.driver_id,
            trip_id=bucket.trip_id,
            window_start=bucket.window_start,
            window_end=window_end,
            avg_speed_kmh=round(avg_speed, 4),
            max_acceleration_ms2=round(max_acceleration, 4),
            harsh_brake_count=harsh_brake_count,
            steering_stddev=round(steering_stddev, 4),
            lane_departure_count=lane_departure_count,
            risk_score=risk_score,
            premium_multiplier=premium_multiplier,
            behavior_class=behavior_class,
        )
