from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

BehaviorLabel = Literal["safe", "aggressive", "distracted"]
TelemetryMode = Literal["production", "demo"]


class SensorEvent(BaseModel):
    vehicle_id: str
    driver_id: str
    trip_id: str
    timestamp: datetime
    sequence_id: int = Field(ge=0)
    sensor_type: str
    value: float
    unit: str
    behavior_label: BehaviorLabel


class SensorBatch(BaseModel):
    sent_at: datetime
    events: list[SensorEvent]


class AggregatedWindow(BaseModel):
    vehicle_id: str
    driver_id: str
    trip_id: str
    window_start: datetime
    window_end: datetime
    mode: TelemetryMode = "production"
    demo_session_id: Optional[str] = None
    avg_speed_kmh: float
    avg_acceleration_ms2: float
    max_acceleration_ms2: float
    avg_brake_intensity: float = Field(ge=0.0)
    avg_steering_variability: float = Field(ge=0.0)
    avg_lane_deviation_m: float = Field(ge=0.0)
    harsh_brake_count: int
    steering_stddev: float
    lane_departure_count: int
    risk_score: float = Field(ge=0.0, le=1.0)
    premium_multiplier: float = Field(ge=1.0)
    behavior_class: BehaviorLabel
