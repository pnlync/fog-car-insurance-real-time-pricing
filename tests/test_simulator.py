from datetime import datetime, timedelta, timezone

from edge.dataset_loader import load_behavior_rows
from edge.sensors import FleetConfig, FleetSimulator, SensorConfig


def test_simulator_generates_sensor_events():
    grouped_rows = load_behavior_rows("data/sample_vehicle_telemetry.csv")
    config = FleetConfig(
        vehicle_count=1,
        interpolation_window_seconds=5,
        behavior_switch_probability=0.0,
        sensor_configs={
            "speed": SensorConfig(5, "km/h", 0.0, 0, 180),
            "acceleration": SensorConfig(10, "m/s2", 0.0, -6, 6),
        },
    )
    simulator = FleetSimulator(config, grouped_rows, seed=7)
    future = datetime.now(timezone.utc) + timedelta(seconds=1)

    events = simulator.collect_due_events(future)

    assert events
    assert {event.sensor_type for event in events} == {"speed", "acceleration"}
