"""Turning an error into text, whatever the error does, and a message's class into its name."""

from __future__ import annotations

__all__ = ["class_name", "safe_str"]


def class_name(message: object) -> str:
    """Return the dotted name of ``message``'s class — ``module.QualName``.

    What the console shows a message as and selects it by: the name a reader
    finds it under, rather than the name it travels as.
    """
    kind = type(message)
    return f"{kind.__module__}.{kind.__qualname__}"


def safe_str(error: BaseException) -> str:
    """Return ``str(error)``, or a stand-in naming its class when ``__str__`` raises.

    An error is what a handler — or a library under it — raised, never a
    reason for reporting it to fail. This is called from error constructors
    and while a failed message is settled, where a second exception would
    replace the failure with a crash and leave the message unsettled.
    """
    try:
        return str(error)
    except Exception:  # noqa: BLE001 — whatever it raises, the failure is still reported
        kind = type(error)
        return f"[unprintable {kind.__module__}.{kind.__qualname__}]"
