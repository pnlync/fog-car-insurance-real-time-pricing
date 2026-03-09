from __future__ import annotations

from pathlib import Path

from pydantic import ValidationError

from common.models import AggregatedWindow


class LocalSpool:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, payload: AggregatedWindow) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(payload.model_dump_json())
            handle.write("\n")

    def replay(self, publish_fn) -> int:
        if not self.path.exists():
            return 0

        pending_lines = self.path.read_text(encoding="utf-8").splitlines()
        if not pending_lines:
            return 0

        remaining: list[str] = []
        replayed = 0
        for line in pending_lines:
            try:
                payload = AggregatedWindow.model_validate_json(line)
            except ValidationError:
                continue
            if publish_fn(payload):
                replayed += 1
            else:
                remaining.append(line)

        self.path.write_text(
            "\n".join(remaining) + ("\n" if remaining else ""), encoding="utf-8"
        )
        return replayed
