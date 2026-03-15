from __future__ import annotations

import json
import logging
from collections import deque
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from hfqt.config import AppConfig


_STANDARD_RECORD_KEYS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}

_CONFIGURED_SIGNATURE: tuple[str, ...] | None = None


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, set):
        return sorted(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "value"):
        return getattr(value, "value")
    return str(value)


def _build_log_payload(record: logging.LogRecord) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
        "level": record.levelname,
        "logger": record.name,
        "message": record.getMessage(),
    }
    for key, value in record.__dict__.items():
        if key in _STANDARD_RECORD_KEYS or key.startswith("_"):
            continue
        payload[key] = value
    if record.exc_info:
        payload["exception"] = logging.Formatter().formatException(record.exc_info)
    return payload


class JsonLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(_build_log_payload(record), ensure_ascii=False, default=_json_default)


class TextLineFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = _build_log_payload(record)
        base = f"{payload['ts']} | {payload['level']:<7} | {payload['logger']} | {payload['message']}"
        extras = {
            key: value
            for key, value in payload.items()
            if key not in {"ts", "level", "logger", "message", "exception"}
        }
        if extras:
            rendered = json.dumps(extras, ensure_ascii=False, default=_json_default)
            base = f"{base} | {rendered}"
        if "exception" in payload:
            base = f"{base}\n{payload['exception']}"
        return base


def _level_name(value: str) -> int:
    return getattr(logging, value.upper(), logging.INFO)


def resolve_log_paths(config: AppConfig) -> dict[str, Path]:
    log_dir = Path(config.log_dir)
    return {
        "dir": log_dir,
        "app": log_dir / config.app_log_filename,
        "trade": log_dir / config.trade_log_filename,
        "error": log_dir / config.error_log_filename,
        "decision": log_dir / config.decision_log_filename,
    }


def setup_logging(config: AppConfig) -> dict[str, Path]:
    global _CONFIGURED_SIGNATURE

    paths = resolve_log_paths(config)
    paths["dir"].mkdir(parents=True, exist_ok=True)
    signature = (
        str(paths["app"].resolve()),
        str(paths["trade"].resolve()),
        str(paths["error"].resolve()),
        str(paths["decision"].resolve()),
        config.log_level.upper(),
        str(config.log_max_bytes),
        str(config.log_backup_count),
    )
    if _CONFIGURED_SIGNATURE == signature:
        return paths

    runtime_logger = logging.getLogger("hfqt")
    runtime_logger.handlers.clear()
    runtime_logger.setLevel(_level_name(config.log_level))
    runtime_logger.propagate = False

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(_level_name(config.log_level))
    stream_handler.setFormatter(TextLineFormatter())
    runtime_logger.addHandler(stream_handler)

    app_handler = RotatingFileHandler(
        paths["app"],
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    app_handler.setLevel(_level_name(config.log_level))
    app_handler.setFormatter(JsonLineFormatter())
    runtime_logger.addHandler(app_handler)

    error_handler = RotatingFileHandler(
        paths["error"],
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(JsonLineFormatter())
    runtime_logger.addHandler(error_handler)

    trade_logger = logging.getLogger("hfqt.trade")
    trade_logger.handlers.clear()
    trade_logger.setLevel(logging.INFO)
    trade_logger.propagate = False

    trade_handler = RotatingFileHandler(
        paths["trade"],
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    trade_handler.setLevel(logging.INFO)
    trade_handler.setFormatter(JsonLineFormatter())
    trade_logger.addHandler(trade_handler)

    decision_logger = logging.getLogger("hfqt.decision")
    decision_logger.handlers.clear()
    decision_logger.setLevel(logging.INFO)
    decision_logger.propagate = False

    decision_handler = RotatingFileHandler(
        paths["decision"],
        maxBytes=config.log_max_bytes,
        backupCount=config.log_backup_count,
        encoding="utf-8",
    )
    decision_handler.setLevel(logging.INFO)
    decision_handler.setFormatter(JsonLineFormatter())
    decision_logger.addHandler(decision_handler)

    _CONFIGURED_SIGNATURE = signature
    runtime_logger.info(
        "runtime logging configured",
        extra={
            "event": "logging_configured",
            "log_dir": str(paths["dir"]),
            "app_log": str(paths["app"]),
            "trade_log": str(paths["trade"]),
            "error_log": str(paths["error"]),
            "decision_log": str(paths["decision"]),
            "log_level": config.log_level.upper(),
        },
    )
    return paths


def get_logger(name: str) -> logging.Logger:
    logger_name = name if name.startswith("hfqt") else f"hfqt.{name}"
    return logging.getLogger(logger_name)


def log_trade_event(event_name: str, **payload: Any) -> None:
    logging.getLogger("hfqt.trade").info(event_name, extra={"event": event_name, "data": payload})


def log_decision_event(event_name: str, **payload: Any) -> None:
    logging.getLogger("hfqt.decision").info(event_name, extra={"event": event_name, "data": payload})


def read_recent_trade_logs(config: AppConfig, limit: int = 50) -> list[dict[str, Any]]:
    log_path = resolve_log_paths(config)["trade"]
    if not log_path.exists():
        return []
    lines: deque[str] = deque(maxlen=max(limit, 1))
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            clean = line.strip()
            if clean:
                lines.append(clean)
    records: list[dict[str, Any]] = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            records.append({"message": line})
    return records
