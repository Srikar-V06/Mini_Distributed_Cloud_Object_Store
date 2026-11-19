import json
import logging
from datetime import datetime, timezone


def event(name: str, **fields) -> dict:
    return {"event_name": name, "event_fields": fields}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "request_id": getattr(record, "request_id", "-"),
            "event": getattr(record, "event_name", "log"),
            "message": record.getMessage(),
        }
        payload.update(getattr(record, "event_fields", {}))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)
