"""Encoding frozen dataclass messages, via msgspec."""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, cast, final, get_type_hints

import msgspec
from typing_extensions import override

from xtr_messenger.exception import MessageDecodingFailedError, MessageEncodingFailedError

from .message_codec_interface import MessageCodecInterface

if TYPE_CHECKING:
    from _typeshed import DataclassInstance

    from .message_codec_interface import JsonValue

__all__ = ["DataclassCodec"]


@final
class DataclassCodec(MessageCodecInterface):
    """Handles frozen dataclass messages, with no third-party modelling layer.

    Type-strict on the way in: a field of the wrong JSON type is an error
    rather than a coercion, so ``"3"`` is never read as an ``int``, and a
    missing required field fails outright.

    A field the message does not declare is **ignored by default**, which is
    what lets a producer add one without redeploying every consumer first.
    Rejecting them would make every schema addition a coordinated deploy, so
    tolerance is the default a bus wants.

    Pass ``forbid_unknown_fields=True`` where the two sides ship together and
    a stray field means a typo rather than a newer producer. It rejects at
    every level of nesting, naming the field and its JSON path.

    It validates *shape*, not domain rules — for constraints like "amount
    must be positive", model the message with pydantic and add
    :class:`~xtr_messenger.transport.serialization.codec.pydantic_codec.PydanticCodec`.
    """

    __slots__ = ("_forbid_unknown_fields", "_strict_mirrors")

    def __init__(self, forbid_unknown_fields: bool = False) -> None:
        """Reject fields the message does not declare when asked to."""
        self._forbid_unknown_fields = forbid_unknown_fields
        self._strict_mirrors: dict[type, type[msgspec.Struct]] = {}

    @override
    def supports(self, message_type: type) -> bool:
        """Report whether ``message_type`` is a dataclass."""
        return dataclasses.is_dataclass(message_type)

    @override
    def encode(self, message: object) -> JsonValue:
        """Render ``message`` field by field.

        Raises:
            MessageEncodingFailedError: If a field type has no encoding.
        """
        try:
            return cast("JsonValue", msgspec.to_builtins(message, str_keys=True))
        except (TypeError, NotImplementedError) as exc:
            raise MessageEncodingFailedError(str(exc), type(message).__name__) from exc

    @override
    def decode(self, message_type: type, raw: JsonValue) -> object:
        """Rebuild ``message_type``, refusing anything that does not fit.

        Raises:
            MessageDecodingFailedError: If a field is missing or malformed,
                or unexpected when unknown fields are forbidden.
        """
        name = message_type.__name__
        try:
            if self._forbid_unknown_fields:
                _ = msgspec.convert(raw, type=self._mirror_of(message_type), strict=True)
            # basedpyright sees msgspec.convert as returning Any and wants this
            # narrowed; ty resolves it precisely and calls the cast redundant.
            return cast("object", msgspec.convert(raw, type=message_type, strict=True))  # ty: ignore[redundant-cast]
        except (msgspec.ValidationError, TypeError, NotImplementedError) as exc:
            raise MessageDecodingFailedError(str(exc), name) from exc

    def _mirror_of(self, message_type: type) -> type[msgspec.Struct]:
        """Return a struct mirroring ``message_type`` that forbids extra fields.

        msgspec offers ``forbid_unknown_fields`` on :class:`msgspec.Struct`
        only, and messages here are the caller's own dataclasses. Mirroring
        the shape onto a struct borrows the check — including its nesting and
        its JSON paths — rather than reimplementing it.
        """
        known = self._strict_mirrors.get(message_type)
        if known is None:
            known = _mirror(message_type, self._strict_mirrors)
        return known


def _mirror(message_type: type, seen: dict[type, type[msgspec.Struct]]) -> type[msgspec.Struct]:
    placeholder = seen.get(message_type)
    if placeholder is not None:
        return placeholder
    hints = get_type_hints(message_type)
    fields: list[object] = []
    for field in dataclasses.fields(cast("type[DataclassInstance]", message_type)):
        annotation = cast("type", hints[field.name])
        if dataclasses.is_dataclass(annotation):
            annotation = _mirror(annotation, seen)
        if field.default is dataclasses.MISSING:
            fields.append((field.name, annotation))
        else:
            fields.append((field.name, annotation, field.default))
    mirrored = msgspec.defstruct(
        message_type.__name__,
        cast("list[tuple[str, type]]", fields),
        forbid_unknown_fields=True,
    )
    seen[message_type] = mirrored
    return mirrored
