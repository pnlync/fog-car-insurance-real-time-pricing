from cloud.demo_mode.session_store import deserialize_session, serialize_session


def test_demo_session_round_trip_preserves_nested_vehicle_profiles():
    session = {
        "demo_session_id": "demo-123",
        "status": "RUNNING",
        "ticks_completed": 1,
        "vehicles": [
            {
                "vehicle_id": "demo-veh-001",
                "profile_a": {"avg_speed_kmh": 48.5},
                "profile_b": {"avg_speed_kmh": 52.1},
                "segment_step": 0,
                "segment_steps": 3,
            }
        ],
    }

    stored = serialize_session(session)
    hydrated = deserialize_session(stored)

    assert "vehicles_json" in stored
    assert hydrated == session
