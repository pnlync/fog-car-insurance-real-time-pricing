from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

import boto3

from cloud.demo_mode.session_store import deserialize_session

DATABASE_NAME = os.getenv("DATABASE_NAME", "driver_pricing")
TABLE_NAME = os.getenv("TABLE_NAME", "telemetry_windows")
DEMO_SESSIONS_TABLE = os.getenv("DEMO_SESSIONS_TABLE", "demo_sessions")
INT_FIELDS = {"harsh_brake_count", "lane_departure_count"}
FLOAT_FIELDS = {
    "avg_speed_kmh",
    "max_acceleration_ms2",
    "steering_stddev",
    "risk_score",
    "premium_multiplier",
}


def _escape_literal(value: str) -> str:
    return value.replace("'", "''")


def _parse_scalar_value(cell: dict[str, Any]) -> Any:
    if "NullValue" in cell and cell["NullValue"]:
        return None
    if "ScalarValue" in cell:
        return cell["ScalarValue"]
    return None


def _parse_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    columns = [column["Name"] for column in result["ColumnInfo"]]
    rows: list[dict[str, Any]] = []
    for row in result["Rows"]:
        values = [_parse_scalar_value(cell) for cell in row["Data"]]
        parsed_row = dict(zip(columns, values))
        for field_name in INT_FIELDS:
            if parsed_row.get(field_name) is not None:
                parsed_row[field_name] = int(parsed_row[field_name])
        for field_name in FLOAT_FIELDS:
            if parsed_row.get(field_name) is not None:
                parsed_row[field_name] = float(parsed_row[field_name])
        rows.append(parsed_row)
    return rows


@lru_cache(maxsize=1)
def get_query_client():
    return boto3.client("timestream-query")


@lru_cache(maxsize=1)
def get_dynamodb_resource():
    return boto3.resource("dynamodb")


def list_vehicles() -> list[str]:
    query = (
        f'SELECT DISTINCT vehicle_id FROM "{DATABASE_NAME}"."{TABLE_NAME}" '
        "WHERE mode = 'production' "
        "ORDER BY vehicle_id"
    )
    result = get_query_client().query(QueryString=query)
    return [row["vehicle_id"] for row in _parse_rows(result)]


def latest_metrics(vehicle_id: str) -> dict[str, Any] | None:
    query = f"""
    SELECT time, vehicle_id, behavior_class, avg_speed_kmh, max_acceleration_ms2,
           harsh_brake_count, steering_stddev, lane_departure_count,
           risk_score, premium_multiplier
    FROM "{DATABASE_NAME}"."{TABLE_NAME}"
    WHERE measure_name = 'trip_window'
      AND mode = 'production'
      AND vehicle_id = '{_escape_literal(vehicle_id)}'
    ORDER BY time DESC
    LIMIT 1
    """
    result = get_query_client().query(QueryString=query)
    rows = _parse_rows(result)
    return rows[0] if rows else None


def recent_metrics(vehicle_id: str, minutes: int = 30) -> list[dict[str, Any]]:
    query = f"""
    SELECT time, avg_speed_kmh, risk_score, harsh_brake_count,
           lane_departure_count, premium_multiplier
    FROM "{DATABASE_NAME}"."{TABLE_NAME}"
    WHERE measure_name = 'trip_window'
      AND mode = 'production'
      AND vehicle_id = '{_escape_literal(vehicle_id)}'
      AND time > ago({minutes}m)
    ORDER BY time ASC
    """
    result = get_query_client().query(QueryString=query)
    return _parse_rows(result)


def latest_demo_metrics(demo_session_id: str) -> dict[str, Any] | None:
    query = f"""
    SELECT time, vehicle_id, behavior_class, avg_speed_kmh, max_acceleration_ms2,
           harsh_brake_count, steering_stddev, lane_departure_count,
           risk_score, premium_multiplier
    FROM "{DATABASE_NAME}"."{TABLE_NAME}"
    WHERE measure_name = 'trip_window'
      AND mode = 'demo'
      AND demo_session_id = '{_escape_literal(demo_session_id)}'
    ORDER BY time DESC
    LIMIT 1
    """
    result = get_query_client().query(QueryString=query)
    rows = _parse_rows(result)
    return rows[0] if rows else None


def recent_demo_metrics(
    demo_session_id: str, minutes: int = 30
) -> list[dict[str, Any]]:
    query = f"""
    SELECT time, avg_speed_kmh, risk_score, harsh_brake_count,
           lane_departure_count, premium_multiplier
    FROM "{DATABASE_NAME}"."{TABLE_NAME}"
    WHERE measure_name = 'trip_window'
      AND mode = 'demo'
      AND demo_session_id = '{_escape_literal(demo_session_id)}'
      AND time > ago({minutes}m)
    ORDER BY time ASC
    """
    result = get_query_client().query(QueryString=query)
    return _parse_rows(result)


def get_demo_session(demo_session_id: str) -> dict[str, Any] | None:
    item = (
        get_dynamodb_resource()
        .Table(DEMO_SESSIONS_TABLE)
        .get_item(Key={"demo_session_id": demo_session_id})
        .get("Item")
    )
    return deserialize_session(item)
