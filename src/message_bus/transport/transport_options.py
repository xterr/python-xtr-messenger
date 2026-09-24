"""Reading adapter settings out of a DSN's query string."""

from __future__ import annotations

from typing import TYPE_CHECKING

from message_bus.exception import InvalidTransportOptionError, UnknownTransportOptionError

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping

__all__ = [
    "as_bool",
    "as_choice",
    "as_float",
    "as_int",
    "as_optional_float",
    "as_optional_int",
    "reject_unknown_options",
]


def reject_unknown_options(
    scheme: str,
    options: Mapping[str, str],
    known: Collection[str],
) -> None:
    """Fail on any option the adapter does not recognise.

    Raises:
        UnknownTransportOptionError: If ``options`` names anything outside
            ``known``.
    """
    unknown = tuple(sorted(set(options) - set(known)))
    if unknown:
        raise UnknownTransportOptionError(scheme, unknown, tuple(sorted(known)))


def as_int(options: Mapping[str, str], key: str, fallback: int) -> int:
    """Read ``key`` as an integer, or return ``fallback``.

    Raises:
        InvalidTransportOptionError: If the value is present but not an integer.
    """
    raw = options.get(key)
    if raw is None:
        return fallback
    try:
        return int(raw)
    except ValueError as exc:
        raise InvalidTransportOptionError(key, raw, "a whole number") from exc


def as_float(options: Mapping[str, str], key: str, fallback: float) -> float:
    """Read ``key`` as a float, or return ``fallback``.

    Raises:
        InvalidTransportOptionError: If the value is present but not a number.
    """
    raw = options.get(key)
    if raw is None:
        return fallback
    try:
        return float(raw)
    except ValueError as exc:
        raise InvalidTransportOptionError(key, raw, "a number") from exc


def as_bool(options: Mapping[str, str], key: str, fallback: bool = False) -> bool:
    """Read ``key`` as a boolean, or return ``fallback``.

    Accepts only ``true`` and ``false``, so a value like ``yes`` fails rather
    than being read as one of them.

    Raises:
        InvalidTransportOptionError: If the value is present but not ``true``/``false``.
    """
    raw = options.get(key)
    if raw is None:
        return fallback
    if raw not in ("true", "false"):
        raise InvalidTransportOptionError(key, raw, "'true' or 'false'")
    return raw == "true"


def as_choice(
    settings: Mapping[str, str],
    key: str,
    allowed: Collection[str],
    fallback: str,
) -> str:
    """Read ``key`` as one of ``allowed``, or return ``fallback``.

    Raises:
        InvalidTransportOptionError: If the value is present but not one of ``allowed``.
    """
    raw = settings.get(key)
    if raw is None:
        return fallback
    if raw not in allowed:
        permitted = ", ".join(sorted(allowed))
        raise InvalidTransportOptionError(key, raw, f"one of: {permitted}")
    return raw


def as_optional_int(settings: Mapping[str, str], key: str, fallback: int | None) -> int | None:
    """Read ``key`` as an integer, or return ``fallback`` when absent."""
    if key not in settings:
        return fallback
    return as_int(settings, key, 0)


def as_optional_float(
    settings: Mapping[str, str], key: str, fallback: float | None
) -> float | None:
    """Read ``key`` as a float, or return ``fallback`` when absent."""
    if key not in settings:
        return fallback
    return as_float(settings, key, 0.0)
