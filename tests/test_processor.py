from datetime import datetime, timedelta, timezone

from common.models import SensorBatch, SensorEvent
from common.pricing import RiskThresholds
from fog.processor import FogProcessor, ProcessorConfig


def test_processor_builds_aggregated_window():
    now = datetime.now(timezone.utc).replace(microsecond=0)
    batch = SensorBatch(
        sent_at=now,
        events=[
            SensorEvent(
                vehicle_id="veh-001",
                driver_id="drv-001",
                trip_id="trip-001",
                timestamp=now - timedelta(seconds=6),
                sequence_id=1,
                sensor_type="speed",
                value=70,
                unit="km/h",
                behavior_label="safe",
            ),
            SensorEvent(
                vehicle_id="veh-001",
                driver_id="drv-001",
                trip_id="trip-001",
                timestamp=now - timedelta(seconds=6),
                sequence_id=2,
                sensor_type="acceleration",
                value=2.2,
                unit="m/s2",
                behavior_label="safe",
            ),
            SensorEvent(
                vehicle_id="veh-001",
                driver_id="drv-001",
                trip_id="trip-001",
                timestamp=now - timedelta(seconds=6),
                sequence_id=3,
                sensor_type="brake_intensity",
                value=0.7,
                unit="ratio",
                behavior_label="safe",
            ),
        ],
    )
    processor = FogProcessor(
        ProcessorConfig(
            window_seconds=5,
            harsh_brake_threshold=0.6,
            lane_departure_threshold=0.4,
            thresholds=RiskThresholds(),
        )
    )

    processor.consume_batch(batch)
    windows = processor.flush_ready(now)

    assert len(windows) == 1
    assert windows[0].harsh_brake_count == 1
    assert windows[0].avg_speed_kmh == 70
