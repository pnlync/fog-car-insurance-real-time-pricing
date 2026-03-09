from cloud.demo_mode.simulator import advance_vehicle_state, initialize_vehicle_state


def test_demo_simulator_generates_demo_payload():
    state = initialize_vehicle_state(
        "demo-session-1234", vehicle_index=0, segment_steps=3
    )

    payload, next_state = advance_vehicle_state(
        demo_session_id="demo-session-1234",
        vehicle_state=state,
        tick_index=0,
        window_seconds=5,
    )

    assert payload["mode"] == "demo"
    assert payload["demo_session_id"] == "demo-session-1234"
    assert payload["vehicle_id"].startswith("demo-veh-")
    assert "avg_brake_intensity" in payload
    assert "avg_lane_deviation_m" in payload
    assert next_state["vehicle_id"] == state["vehicle_id"]
