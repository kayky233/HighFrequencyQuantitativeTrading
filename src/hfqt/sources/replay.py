from __future__ import annotations

import json
from pathlib import Path

from hfqt.schemas import InputEvent


class ReplayEventSource:
    async def load(self, path: str | Path) -> InputEvent:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return InputEvent.model_validate(payload)
