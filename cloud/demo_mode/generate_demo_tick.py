from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3

from .session_store import deserialize_session, serialize_session
from .simulator import advance_vehicle_state

DEMO_SESSIONS_TABLE = os.environ["DEMO_SESSIONS_TABLE"]
TELEMETRY_QUEUE_URL = os.environ["TELEMETRY_QUEUE_URL"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    demo_session_id = event["demo_session_id"]
    table = boto3.resource("dynamodb").Table(DEMO_SESSIONS_TABLE)
    session = deserialize_session(
        table.get_item(Key={"demo_session_id": demo_session_id}).get("Item")
    )
    if not session:
        return {"demo_session_id": demo_session_id, "continue": False}

    if session.get("status") in {"STOPPED", "COMPLETED"}:
        return {"demo_session_id": demo_session_id, "continue": False}

    if session.get("stop_requested"):
        session["status"] = "STOPPED"
        session["updated_at"] = _utc_now_iso()
        table.put_item(Item=serialize_session(session))
        return {"demo_session_id": demo_session_id, "continue": False}

    tick_index = int(session.get("ticks_completed", 0))
    max_ticks = int(session.get("max_ticks", 0))
    if tick_index >= max_ticks:
        session["status"] = "COMPLETED"
        session["updated_at"] = _utc_now_iso()
        table.put_item(Item=serialize_session(session))
        return {"demo_session_id": demo_session_id, "continue": False}

    sqs = boto3.client("sqs")
    next_states = []
    for vehicle_state in session["vehicles"]:
        payload, next_state = advance_vehicle_state(
            demo_session_id=demo_session_id,
            vehicle_state=vehicle_state,
            tick_index=tick_index,
            window_seconds=int(session["window_seconds"]),
        )
        sqs.send_message(
            QueueUrl=TELEMETRY_QUEUE_URL,
            MessageBody=json.dumps(payload),
        )
        next_states.append(next_state)

    session["vehicles"] = next_states
    session["ticks_completed"] = tick_index + 1
    session["updated_at"] = _utc_now_iso()
    session["status"] = (
        "COMPLETED"
        if int(session["ticks_completed"]) >= max_ticks
        else "RUNNING"
    )
    session["last_window_end"] = session["updated_at"]
    table.put_item(Item=serialize_session(session))
    return {
        "demo_session_id": demo_session_id,
        "continue": session["status"] == "RUNNING",
    }
