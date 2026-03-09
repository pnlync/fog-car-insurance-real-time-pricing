from edge.dataset_loader import load_behavior_rows


def test_loader_supports_driver_behavior_style_columns(tmp_path):
    csv_path = tmp_path / "driver_behavior.csv"
    csv_path.write_text(
        "\n".join(
            [
                "speed_kmph,accel_x,accel_y,brake_pressure,steering_angle,throttle,lane_deviation,phone_usage,headway_distance,reaction_time,behavior_label",
                "36.0,0.5,0.7,23.0,-3.1,53.0,0.85,1,18.0,1.4,Distracted",
                "58.0,1.2,0.2,41.0,10.0,61.0,0.22,0,25.0,1.0,Safe",
                "91.0,3.0,0.4,88.0,-22.0,81.0,1.52,0,12.0,0.7,Aggressive",
            ]
        ),
        encoding="utf-8",
    )

    grouped = load_behavior_rows(csv_path)

    distracted_row = grouped["distracted"][0]
    aggressive_row = grouped["aggressive"][0]

    assert grouped["safe"]
    assert distracted_row.speed == 36.0
    assert distracted_row.acceleration > 0.5
    assert 0.0 <= distracted_row.brake_intensity <= 1.0
    assert 0.0 <= distracted_row.steering_variability <= 3.0
    assert aggressive_row.lane_deviation == 1.52
