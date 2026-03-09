from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from typing import Any

import boto3
from boto3.dynamodb.conditions import Attr, Key

from cloud.demo_mode.session_store import deserialize_session

TELEMETRY_TABLE_NAME = os.getenv("TELEMETRY_TABLE_NAME", "telemetry_windows")
DEMO_SESSIONS_TABLE = os.getenv("DEMO_SESSIONS_TABLE", "demo_sessions")


def _iso_now_minus_minutes(minutes: int) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return cutoff.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _convert_dynamodb_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        integral = value.to_integral_value()
        return int(value) if value == integral else float(value)
    if isinstance(value, dict):
        return {
            key: _convert_dynamodb_value(nested_value)
            for key, nested_value in value.items()
        }
    if isinstance(value, list):
        return [_convert_dynamodb_value(nested_value) for nested_value in value]
    return value


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    normalized = _convert_dynamodb_value(item)
    normalized.setdefault(
        "avg_acceleration_ms2",
        normalized.get("max_acceleration_ms2"),
    )
    normalized.setdefault("avg_brake_intensity", None)
    normalized.setdefault(
        "avg_steering_variability",
        normalized.get("steering_stddev"),
    )
    normalized.setdefault("avg_lane_deviation_m", None)
    normalized["time"] = normalized["window_end"]
    return normalized


def _query_partition(
    partition_key: str,
    *,
    minutes: int | None = None,
    descending: bool = False,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    query_kwargs: dict[str, Any] = {
        "KeyConditionExpression": Key("pk").eq(partition_key),
        "ScanIndexForward": not descending,
    }
    if minutes is not None:
        query_kwargs["KeyConditionExpression"] = Key("pk").eq(partition_key) & Key(
            "sk"
        ).gte(_iso_now_minus_minutes(minutes))
    if limit is not None:
        query_kwargs["Limit"] = limit

    response = get_telemetry_table().query(**query_kwargs)
    return [_normalize_item(item) for item in response.get("Items", [])]


@lru_cache(maxsize=1)
def get_dynamodb_resource():
    return boto3.resource("dynamodb")


@lru_cache(maxsize=1)
def get_telemetry_table():
    return get_dynamodb_resource().Table(TELEMETRY_TABLE_NAME)


def list_vehicles() -> list[str]:
    vehicles: set[str] = set()
    scan_kwargs: dict[str, Any] = {
        "ProjectionExpression": "vehicle_id, #mode",
        "FilterExpression": Attr("mode").eq("production"),
        "ExpressionAttributeNames": {"#mode": "mode"},
    }
    response = get_telemetry_table().scan(**scan_kwargs)
    for item in response.get("Items", []):
        vehicles.add(item["vehicle_id"])

    while "LastEvaluatedKey" in response:
        response = get_telemetry_table().scan(
            **scan_kwargs,
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        for item in response.get("Items", []):
            vehicles.add(item["vehicle_id"])

    return sorted(vehicles)


def latest_metrics(vehicle_id: str) -> dict[str, Any] | None:
    items = _query_partition(f"VEHICLE#{vehicle_id}", descending=True, limit=1)
    return items[0] if items else None


def recent_metrics(vehicle_id: str, minutes: int = 10) -> list[dict[str, Any]]:
    return _query_partition(f"VEHICLE#{vehicle_id}", minutes=minutes)


def latest_demo_metrics(demo_session_id: str) -> dict[str, Any] | None:
    items = _query_partition(f"DEMO#{demo_session_id}", descending=True, limit=1)
    return items[0] if items else None


def recent_demo_metrics(
    demo_session_id: str, minutes: int = 10
) -> list[dict[str, Any]]:
    return _query_partition(f"DEMO#{demo_session_id}", minutes=minutes)


def get_demo_session(demo_session_id: str) -> dict[str, Any] | None:
    item = (
        get_dynamodb_resource()
        .Table(DEMO_SESSIONS_TABLE)
        .get_item(Key={"demo_session_id": demo_session_id})
        .get("Item")
    )
    return deserialize_session(item)
