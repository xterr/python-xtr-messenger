"""Every message's records held back until one of them is an error, then handed to a recorder."""

from __future__ import annotations

from xtr_dependency_injection import configure
from xtr_logging import LoggingConfig
from xtr_logging.config import FingersCrossedHandlerConfig, ServiceHandlerConfig

__all__ = ["logging"]


@configure
def logging() -> LoggingConfig:
    return LoggingConfig(
        handlers={
            "held": FingersCrossedHandlerConfig(action_level="error", handler="recorded"),
            "recorded": ServiceHandlerConfig(id="recorded"),
        },
    )
