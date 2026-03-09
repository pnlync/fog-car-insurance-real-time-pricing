import json

from cloud.dashboard_lambda import app as dashboard_lambda


def test_dashboard_route_returns_html():
    response = dashboard_lambda.handler(
        {
            "rawPath": "/",
            "requestContext": {"http": {"method": "GET"}},
        },
        None,
    )

    assert response["statusCode"] == 200
    assert "text/html" in response["headers"]["Content-Type"]
    assert "Real-Time Car Insurance Pricing" in response["body"]


def test_vehicles_route_returns_json(monkeypatch):
    monkeypatch.setattr(
        dashboard_lambda,
        "list_vehicles",
        lambda: ["veh-001", "veh-002"],
    )

    response = dashboard_lambda.handler(
        {
            "rawPath": "/api/vehicles",
            "requestContext": {"http": {"method": "GET"}},
        },
        None,
    )

    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == {"vehicles": ["veh-001", "veh-002"]}
