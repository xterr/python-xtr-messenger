"""What a profile never keeps the value of: a credential, by the name it is kept under."""

from __future__ import annotations

from typing import Final, cast

from xtr_http_kernel.data_collector import clone_var

__all__ = ["MASK", "is_sensitive", "masked"]

MASK: Final = "******"

_SENSITIVE_HEADERS: Final = frozenset(
    {"authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key"}
)
_SENSITIVE_FIELDS: Final = ("password", "secret", "token", "passwd", "api_key", "apikey")


def is_sensitive(name: str) -> bool:
    """Tell whether a field, key or header with this name likely holds a credential."""
    lowered = name.lower()
    return lowered in _SENSITIVE_HEADERS or any(word in lowered for word in _SENSITIVE_FIELDS)


def masked(node: dict[str, object]) -> dict[str, object]:
    """Return a :func:`clone_var` tree with every credential-named value replaced by the mask.

    A message is a plain object a publisher filled in, and a profile is shown
    to whoever opens the profiler, so an attribute or a mapping key named like
    a credential keeps its name and loses its value — at any depth the clone
    reached.
    """
    match node.get("kind"):
        case "object":
            attributes = cast("list[tuple[str, dict[str, object]]]", node["attributes"])
            return {
                **node,
                "attributes": [[name, _value(name, child)] for name, child in attributes],
            }
        case "mapping":
            pairs = cast("list[tuple[dict[str, object], dict[str, object]]]", node["items"])
            return {
                **node,
                "items": [[key, _value(_key_name(key), child)] for key, child in pairs],
            }
        case "sequence":
            items = cast("list[dict[str, object]]", node["items"])
            return {**node, "items": [masked(child) for child in items]}
        case _:
            return node


def _value(name: str | None, child: dict[str, object]) -> dict[str, object]:
    if name is not None and is_sensitive(name):
        return clone_var(MASK)
    return masked(child)


def _key_name(key: dict[str, object]) -> str | None:
    value = key.get("value")
    return value if key.get("kind") == "str" and isinstance(value, str) else None
