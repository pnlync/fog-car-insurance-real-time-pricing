from decimal import Decimal

from cloud.lambda_ingest.app import build_item, validate_payload


def sample_payload(**overrides):
    payload = {
        "vehicle_id": "veh-001",
        "driver_id": "drv-001",
        "trip_id": "trip-001",
        "window_start": "2026-03-09T21:00:00Z",
        "window_end": "2026-03-09T21:00:05Z",
        "avg_speed_kmh": 65.5,
        "avg_acceleration_ms2": 1.4,
        "max_acceleration_ms2": 2.1,
        "avg_brake_intensity": 0.42,
        "avg_steering_variability": 0.36,
        "avg_lane_deviation_m": 0.18,
        "harsh_brake_count": 1,
        "steering_stddev": 0.22,
        "lane_departure_count": 0,
        "risk_score": 0.32,
        "premium_multiplier": 1.16,
        "behavior_class": "safe",
    }
    payload.update(overrides)
    return payload


def test_validate_payload_defaults_to_production_mode():
    validated = validate_payload(sample_payload())

    assert validated["mode"] == "production"
    assert validated["demo_session_id"] == "production"


def test_build_item_uses_vehicle_partition_for_production():
    item = build_item(validate_payload(sample_payload()))

    assert item["pk"] == "VEHICLE#veh-001"
    assert item["sk"] == "2026-03-09T21:00:05Z#trip-001"
    assert item["avg_speed_kmh"] == Decimal("65.5")
    assert item["avg_brake_intensity"] == Decimal("0.42")


def test_build_item_uses_demo_partition_for_demo_mode():
    payload = sample_payload(mode="demo", demo_session_id="demo-123")
    item = build_item(validate_payload(payload))

    assert item["pk"] == "DEMO#demo-123"
    assert item["demo_session_id"] == "demo-123"
