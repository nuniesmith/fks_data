"""Pydantic schemas and DTOs for the data service (placeholder)."""

try:
	from pydantic import BaseModel
except Exception:  # pragma: no cover - fallback if pydantic unavailable
	class BaseModel:  # type: ignore
		def __init__(self, **kwargs):
			for k, v in kwargs.items():
				setattr(self, k, v)


class HealthResponse(BaseModel):  # type: ignore[misc]
	"""Minimal schema used as a placeholder until real models are added."""
	status: str = "ok"


__all__ = ["HealthResponse"]

