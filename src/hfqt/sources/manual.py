from __future__ import annotations

from hfqt.schemas import InputEvent


class ManualEventSource:
    async def load(self, payload: dict) -> InputEvent:
        return InputEvent.model_validate(payload)
