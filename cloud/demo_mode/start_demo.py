from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3

from .session_store import serialize_session
from .simulator import initialize_vehicle_state

DEMO_SESSIONS_TABLE = os.environ["DEMO_SESSIONS_TABLE"]
DEMO_STATE_MACHINE_ARN = os.environ["DEMO_STATE_MACHINE_ARN"]
DEFAULT_DURATION_MINUTES = int(os.getenv("DEMO_DURATION_MINUTES", "2"))
DEFAULT_VEHICLE_COUNT = int(os.getenv("DEMO_VEHICLE_COUNT", "1"))
WINDOW_SECONDS = int(os.getenv("DEMO_WINDOW_SECONDS", "5"))
SEGMENT_STEPS = int(os.getenv("DEMO_SEGMENT_STEPS", "3"))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    payload = event if isinstance(event, dict) else {}
    duration_minutes = int(payload.get("duration_minutes", DEFAULT_DURATION_MINUTES))
    vehicle_count = int(payload.get("vehicle_count", DEFAULT_VEHICLE_COUNT))
    duration_minutes = max(1, min(duration_minutes, 10))
    vehicle_count = max(1, min(vehicle_count, 3))

    demo_session_id = str(uuid.uuid4())
    now = _utc_now()
    max_ticks = max(1, (duration_minutes * 60) // WINDOW_SECONDS)
    session = {
        "demo_session_id": demo_session_id,
        "status": "STARTING",
        "created_at": now.isoformat().replace("+00:00", "Z"),
        "updated_at": now.isoformat().replace("+00:00", "Z"),
        "expires_at": int((now + timedelta(days=1)).timestamp()),
        "window_seconds": WINDOW_SECONDS,
        "duration_minutes": duration_minutes,
        "vehicle_count": vehicle_count,
        "ticks_completed": 0,
        "max_ticks": max_ticks,
        "stop_requested": False,
        "vehicles": [
            initialize_vehicle_state(demo_session_id, index, SEGMENT_STEPS)
            for index in range(vehicle_count)
        ],
    }

    table = boto3.resource("dynamodb").Table(DEMO_SESSIONS_TABLE)
    table.put_item(Item=serialize_session(session))

    try:
        execution = boto3.client("stepfunctions").start_execution(
            stateMachineArn=DEMO_STATE_MACHINE_ARN,
            name=f"demo-{demo_session_id}",
            input=json.dumps({"demo_session_id": demo_session_id}),
        )
    except Exception:
        session["status"] = "FAILED"
        session["updated_at"] = _utc_now().isoformat().replace("+00:00", "Z")
        table.put_item(Item=serialize_session(session))
        raise

    session["execution_arn"] = execution["executionArn"]
    session["status"] = "RUNNING"
    session["updated_at"] = _utc_now().isoformat().replace("+00:00", "Z")
    table.put_item(Item=serialize_session(session))
    return session
