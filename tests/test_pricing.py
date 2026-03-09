from common.pricing import RiskInputs, compute_premium_multiplier, compute_risk_score


def test_pricing_outputs_stay_in_expected_range():
    inputs = RiskInputs(
        avg_speed_kmh=90,
        max_acceleration_ms2=2.5,
        harsh_brake_count=2,
        steering_stddev=0.6,
        lane_departure_count=1,
    )
    risk_score = compute_risk_score(inputs)
    premium_multiplier = compute_premium_multiplier(risk_score)

    assert 0.0 <= risk_score <= 1.0
    assert premium_multiplier >= 1.0
