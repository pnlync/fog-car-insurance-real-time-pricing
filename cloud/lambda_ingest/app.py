from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import boto3

TELEMETRY_TABLE_NAME = os.getenv("TELEMETRY_TABLE_NAME", "telemetry_windows")

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
INTEGER_FIELDS = {"harsh_brake_count", "lane_departure_count"}
FLOAT_FIELDS = {
    "avg_speed_kmh",
    "max_acceleration_ms2",
    "steering_stddev",
    "risk_score",
    "premium_multiplier",
}


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _to_decimal(value: float | int) -> Decimal:
    return Decimal(str(value))


def _partition_key(payload: dict[str, Any]) -> str:
    if payload["mode"] == "demo":
        return f"DEMO#{payload['demo_session_id']}"
    return f"VEHICLE#{payload['vehicle_id']}"


def _sort_key(payload: dict[str, Any]) -> str:
    return f"{payload['window_end']}#{payload['trip_id']}"


def validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    missing = REQUIRED_FIELDS - payload.keys()
    if missing:
        raise ValueError(f"Missing fields in payload: {sorted(missing)}")

    payload = dict(payload)
    payload.setdefault("mode", "production")
    payload.setdefault("demo_session_id", "production")
    if payload["mode"] == "demo" and not payload["demo_session_id"]:
        raise ValueError("demo_session_id is required when mode=demo")
    return payload


def build_item(payload: dict[str, Any]) -> dict[str, Any]:
    window_end = _parse_iso_datetime(payload["window_end"])
    item = {
        "pk": _partition_key(payload),
        "sk": _sort_key(payload),
        "record_type": "telemetry_window",
        "vehicle_id": payload["vehicle_id"],
        "driver_id": payload["driver_id"],
        "trip_id": payload["trip_id"],
        "window_start": payload["window_start"],
        "window_end": payload["window_end"],
        "mode": payload["mode"],
        "demo_session_id": payload["demo_session_id"],
        "behavior_class": payload["behavior_class"],
        "expires_at": int((window_end + timedelta(days=30)).timestamp()),
    }
    for field_name in INTEGER_FIELDS:
        item[field_name] = int(payload[field_name])
    for field_name in FLOAT_FIELDS:
        item[field_name] = _to_decimal(payload[field_name])
    return item


def handler(event: dict[str, Any], _context: Any) -> dict[str, int]:
    table = boto3.resource("dynamodb").Table(TELEMETRY_TABLE_NAME)
    records_written = 0

    for record in event.get("Records", []):
        payload = validate_payload(json.loads(record["body"]))
        table.put_item(Item=build_item(payload))
        records_written += 1

    return {"records_written": records_written}
