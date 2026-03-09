from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any

import boto3

START_DEMO_FUNCTION_NAME = os.getenv("START_DEMO_FUNCTION_NAME", "")
STOP_DEMO_FUNCTION_NAME = os.getenv("STOP_DEMO_FUNCTION_NAME", "")


@lru_cache(maxsize=1)
def get_lambda_client():
    return boto3.client("lambda")


def _invoke(function_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not function_name:
        return {
            "status": "UNCONFIGURED",
            "message": "Lambda function name was not provided.",
        }

    response = get_lambda_client().invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    raw_payload = response["Payload"].read().decode("utf-8")
    parsed_payload = json.loads(raw_payload or "{}")
    if "FunctionError" in response:
        return {
            "status": "ERROR",
            "message": parsed_payload.get("errorMessage", "Lambda invocation failed."),
            "details": parsed_payload,
        }
    return parsed_payload


def start_demo_session(
    duration_minutes: int = 2, vehicle_count: int = 1
) -> dict[str, Any]:
    return _invoke(
        START_DEMO_FUNCTION_NAME,
        {
            "duration_minutes": duration_minutes,
            "vehicle_count": vehicle_count,
        },
    )


def stop_demo_session(demo_session_id: str) -> dict[str, Any]:
    return _invoke(
        STOP_DEMO_FUNCTION_NAME,
        {
            "demo_session_id": demo_session_id,
        },
    )
