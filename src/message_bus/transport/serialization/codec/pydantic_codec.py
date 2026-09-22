"""Pydantic-modelled messages — validation beyond shape.

Installed with the ``pydantic`` extra. Use this when a message needs rules a
type cannot express: a positive amount, a non-empty list, an email address,
a cross-field invariant. Validation runs on decode, at the boundary where an
untrusted payload arrives.

Strictness is the model's own business. A message that should reject a
string where it wants an integer says so itself::

    class IssueInvoice(BaseModel):
        model_config = ConfigDict(frozen=True, strict=True, extra="forbid")

        amount: Annotated[Decimal, Field(gt=0)]

This codec deliberately does not override that — forcing strict mode would
break validators written to normalise their input.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast, final

from pydantic import BaseModel, ValidationError
from typing_extensions import override

from message_bus.exception import MessageDecodingFailedError

from .message_codec_interface import MessageCodecInterface

if TYPE_CHECKING:
    from .message_codec_interface import JsonValue

__all__ = ["PydanticCodec"]


@final
class PydanticCodec(MessageCodecInterface):
    """Handles messages modelled as pydantic ``BaseModel`` subclasses."""

    __slots__ = ()

    @override
    def supports(self, message_type: type) -> bool:
        """Report whether ``message_type`` is a pydantic model."""
        return issubclass(message_type, BaseModel)

    @override
    def encode(self, message: object) -> JsonValue:
        """Render ``message`` through the model's JSON dump.

        Raises:
            MessageDecodingFailedError: If ``message`` is not a pydantic model.
        """
        if not isinstance(message, BaseModel):
            raise MessageDecodingFailedError(f"{type(message).__name__!r} is not a pydantic model")
        return cast("JsonValue", message.model_dump(mode="json"))

    @override
    def decode(self, message_type: type, raw: JsonValue) -> object:
        """Validate ``raw`` against the model.

        Raises:
            MessageDecodingFailedError: If validation fails, wrapping pydantic's
                error so callers never need to import pydantic to catch it.
        """
        if not issubclass(message_type, BaseModel):
            raise MessageDecodingFailedError(f"{message_type.__name__!r} is not a pydantic model")
        try:
            return message_type.model_validate(raw)
        except ValidationError as exc:
            raise MessageDecodingFailedError(_summarise(exc), message_type.__name__) from exc


def _summarise(error: ValidationError) -> str:
    problems = [
        f"{'.'.join(str(part) for part in item['loc']) or '<root>'}: {item['msg']}"
        for item in error.errors()
    ]
    return "; ".join(problems)
