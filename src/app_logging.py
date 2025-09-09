import logging, json, os

_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
_LOGGER_INITIALIZED = False

class _JsonFormatter(logging.Formatter):
    def format(self, record):  # type: ignore
        data = {
            "level": record.levelname.lower(),
            "name": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            data.update(extra)
        return json.dumps(data)

def init_logging(force: bool = False):  # minimal subset
    global _LOGGER_INITIALIZED
    if _LOGGER_INITIALIZED and not force:
        return
    json_logs = os.getenv("FKS_JSON_LOGS", "0").lower() in ("1","true","yes")
    handlers = []
    if json_logs:
        h = logging.StreamHandler()
        h.setFormatter(_JsonFormatter())
        handlers.append(h)
    else:
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=logging.INFO, format=_FORMAT, handlers=handlers, force=True)
    _LOGGER_INITIALIZED = True

def get_logger(name: str):  # minimal shim
    if not _LOGGER_INITIALIZED:
        init_logging()
    return logging.getLogger(name)

__all__ = ["get_logger", "init_logging"]