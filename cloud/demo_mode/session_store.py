from __future__ import annotations

import json
from copy import deepcopy
from decimal import Decimal
from typing import Any

SESSION_JSON_FIELDS = {"vehicles"}


def _convert_for_dynamodb(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [_convert_for_dynamodb(item) for item in value]
    if isinstance(value, dict):
        return {key: _convert_for_dynamodb(item) for key, item in value.items()}
    return value


def _convert_from_dynamodb(value: Any) -> Any:
    if isinstance(value, Decimal):
        integral = value.to_integral_value()
        return int(value) if value == integral else float(value)
    if isinstance(value, list):
        return [_convert_from_dynamodb(item) for item in value]
    if isinstance(value, dict):
        return {key: _convert_from_dynamodb(item) for key, item in value.items()}
    return value


def serialize_session(session: dict[str, Any]) -> dict[str, Any]:
    stored = deepcopy(session)
    for field_name in SESSION_JSON_FIELDS:
        if field_name in stored:
            stored[f"{field_name}_json"] = json.dumps(stored.pop(field_name))
    return _convert_for_dynamodb(stored)


def deserialize_session(item: dict[str, Any] | None) -> dict[str, Any] | None:
    if not item:
        return None

    session = _convert_from_dynamodb(deepcopy(item))
    for field_name in SESSION_JSON_FIELDS:
        json_field_name = f"{field_name}_json"
        if json_field_name in session:
            session[field_name] = json.loads(session.pop(json_field_name))
    return session
