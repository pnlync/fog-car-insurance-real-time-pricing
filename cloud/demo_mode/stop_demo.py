from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from .session_store import deserialize_session, serialize_session

DEMO_SESSIONS_TABLE = os.environ["DEMO_SESSIONS_TABLE"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    demo_session_id = event["demo_session_id"]
    table = boto3.resource("dynamodb").Table(DEMO_SESSIONS_TABLE)
    session = deserialize_session(
        table.get_item(Key={"demo_session_id": demo_session_id}).get("Item")
    )
    if not session:
        return {
            "demo_session_id": demo_session_id,
            "status": "NOT_FOUND",
        }

    execution_arn = session.get("execution_arn")
    if execution_arn and session.get("status") not in {"STOPPED", "COMPLETED"}:
        try:
            boto3.client("stepfunctions").stop_execution(
                executionArn=execution_arn,
                cause="Stopped from dashboard demo controls",
            )
        except ClientError as exc:
            error_code = exc.response["Error"].get("Code", "")
            if error_code not in {"ExecutionDoesNotExist", "ValidationException"}:
                raise

    session["stop_requested"] = True
    session["status"] = "STOPPED"
    session["updated_at"] = _utc_now_iso()
    table.put_item(Item=serialize_session(session))
    return session
