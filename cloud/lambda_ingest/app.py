from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import boto3

DATABASE_NAME = os.getenv("DATABASE_NAME", "driver_pricing")
TABLE_NAME = os.getenv("TABLE_NAME", "telemetry_windows")


REQUIRED_FIELDS = {
    "vehicle_id",
    "driver_id",
    "trip_id",
    "window_start",
    "window_end",
    "avg_speed_kmh",
    "max_acceleration_ms2",
    "harsh_brake_count",
    "steering_stddev",
    "lane_departure_count",
    "risk_score",
    "premium_multiplier",
    "behavior_class",
}


def validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        raise ValueError(f"Missing fields in payload: {sorted(missing)}")
    payload.setdefault("mode", "production")
    payload.setdefault("demo_session_id", "production")
    return payload


def build_record(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "Dimensions": [
            {"Name": "vehicle_id", "Value": payload["vehicle_id"]},
            {"Name": "driver_id", "Value": payload["driver_id"]},
            {"Name": "trip_id", "Value": payload["trip_id"]},
            {"Name": "behavior_class", "Value": payload["behavior_class"]},
            {"Name": "mode", "Value": payload["mode"]},
            {"Name": "demo_session_id", "Value": payload["demo_session_id"]},
        ],
        "MeasureName": "trip_window",
        "MeasureValueType": "MULTI",
        "MeasureValues": [
            {
                "Name": "avg_speed_kmh",
                "Value": str(payload["avg_speed_kmh"]),
                "Type": "DOUBLE",
            },
            {
                "Name": "max_acceleration_ms2",
                "Value": str(payload["max_acceleration_ms2"]),
                "Type": "DOUBLE",
            },
            {
                "Name": "harsh_brake_count",
                "Value": str(payload["harsh_brake_count"]),
                "Type": "BIGINT",
            },
            {
                "Name": "steering_stddev",
                "Value": str(payload["steering_stddev"]),
                "Type": "DOUBLE",
            },
            {
                "Name": "lane_departure_count",
                "Value": str(payload["lane_departure_count"]),
                "Type": "BIGINT",
            },
            {
                "Name": "risk_score",
                "Value": str(payload["risk_score"]),
                "Type": "DOUBLE",
            },
            {
                "Name": "premium_multiplier",
                "Value": str(payload["premium_multiplier"]),
                "Type": "DOUBLE",
            },
        ],
        "Time": str(
            int(
                datetime.fromisoformat(
                    payload["window_end"].replace("Z", "+00:00")
                ).timestamp()
                * 1000
            )
        ),
        "TimeUnit": "MILLISECONDS",
    }


def handler(event: dict[str, Any], _context: Any) -> dict[str, int]:
    client = boto3.client("timestream-write")
    records_written = 0

    for record in event.get("Records", []):
        payload = validate_payload(json.loads(record["body"]))
        client.write_records(
            DatabaseName=DATABASE_NAME,
            TableName=TABLE_NAME,
            Records=[build_record(payload)],
        )
        records_written += 1

    return {"records_written": records_written}
