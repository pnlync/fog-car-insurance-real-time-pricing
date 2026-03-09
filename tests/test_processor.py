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
            emit_lag_seconds=0,
            thresholds=RiskThresholds(),
        )
    )

    processor.consume_batch(batch)
    windows = processor.flush_ready(now)

    assert len(windows) == 1
    assert windows[0].avg_acceleration_ms2 == 2.2
    assert windows[0].avg_brake_intensity == 0.7
    assert windows[0].harsh_brake_count == 1
    assert windows[0].avg_speed_kmh == 70


def test_processor_waits_for_emit_lag_before_flushing():
    now = datetime(2026, 3, 9, 22, 0, 7, tzinfo=timezone.utc)
    batch = SensorBatch(
        sent_at=now,
        events=[
            SensorEvent(
                vehicle_id="veh-001",
                driver_id="drv-001",
                trip_id="trip-001",
                timestamp=now - timedelta(seconds=4),
                sequence_id=1,
                sensor_type="speed",
                value=64,
                unit="km/h",
                behavior_label="safe",
            )
        ],
    )
    processor = FogProcessor(
        ProcessorConfig(
            window_seconds=5,
            harsh_brake_threshold=0.6,
            lane_departure_threshold=0.4,
            emit_lag_seconds=3,
            thresholds=RiskThresholds(),
        )
    )

    processor.consume_batch(batch)

    assert processor.flush_ready(now) == []
    flushed = processor.flush_ready(now + timedelta(seconds=1))

    assert len(flushed) == 1
    assert flushed[0].avg_speed_kmh == 64
    assert flushed[0].avg_acceleration_ms2 == 0


def test_processor_drops_incomplete_window_without_speed_samples():
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
                sensor_type="brake_intensity",
                value=0.7,
                unit="ratio",
                behavior_label="safe",
            )
        ],
    )
    processor = FogProcessor(
        ProcessorConfig(
            window_seconds=5,
            harsh_brake_threshold=0.6,
            lane_departure_threshold=0.4,
            emit_lag_seconds=0,
            thresholds=RiskThresholds(),
        )
    )

    processor.consume_batch(batch)

    assert processor.flush_ready(now) == []


def test_processor_counts_contiguous_threshold_runs_as_single_event():
    now = datetime(2026, 3, 9, 22, 0, 12, tzinfo=timezone.utc)
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
                sensor_type="brake_intensity",
                value=0.8,
                unit="ratio",
                behavior_label="safe",
            ),
            SensorEvent(
                vehicle_id="veh-001",
                driver_id="drv-001",
                trip_id="trip-001",
                timestamp=now - timedelta(seconds=5, milliseconds=800),
                sequence_id=3,
                sensor_type="brake_intensity",
                value=0.9,
                unit="ratio",
                behavior_label="safe",
            ),
            SensorEvent(
                vehicle_id="veh-001",
                driver_id="drv-001",
                trip_id="trip-001",
                timestamp=now - timedelta(seconds=5, milliseconds=600),
                sequence_id=4,
                sensor_type="brake_intensity",
                value=0.2,
                unit="ratio",
                behavior_label="safe",
            ),
            SensorEvent(
                vehicle_id="veh-001",
                driver_id="drv-001",
                trip_id="trip-001",
                timestamp=now - timedelta(seconds=5, milliseconds=400),
                sequence_id=5,
                sensor_type="brake_intensity",
                value=0.85,
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
            emit_lag_seconds=0,
            thresholds=RiskThresholds(),
        )
    )

    processor.consume_batch(batch)
    windows = processor.flush_ready(now)

    assert len(windows) == 1
    assert windows[0].harsh_brake_count == 2
