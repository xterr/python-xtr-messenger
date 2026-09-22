"""Turning envelopes into something a transport can carry, and back.

An encoded envelope is a ``body`` string plus a flat ``headers`` mapping. The
message's stable name travels in the ``type`` header, and each stamp class
gets its own ``X-Message-Stamp-<Name>`` header.

Owning this boundary is what lets a schema drift surface as
:class:`~message_bus.exception.MessageDecodingFailedError` instead of a
wrongly shaped object arriving deep inside a handler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast, final

import msgspec
from typing_extensions import override

from message_bus.envelope import Envelope
from message_bus.exception import MessageDecodingFailedError, UnknownMessageNameError
from message_bus.message_registry import name_of, type_for_name
from message_bus.stamp import NonSendableStampInterface, StampInterface

from .codec import default_codecs
from .encoded_envelope import EncodedEnvelope
from .serializer_interface import SerializerInterface

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from .codec import MessageCodecInterface
    from .codec.message_codec_interface import JsonValue

__all__ = ["TYPE_HEADER", "JsonSerializer"]

TYPE_HEADER = "type"
STAMP_HEADER_PREFIX = "X-Message-Stamp-"


@final
class JsonSerializer(SerializerInterface):
    """Encodes messages and their stamps as JSON.

    Messages are converted by the first codec that recognises their type.
    By default that is
    :func:`~message_bus.transport.serialization.codec.default_codecs`, which
    handles dataclasses, plus pydantic models when pydantic is installed.
    Pass ``codecs`` to take control of the order or to add your own.

    Only stamp types passed to ``stamp_types`` are restored on decode;
    anything else is dropped rather than guessed at. Stamps inheriting
    :class:`~message_bus.stamp.NonSendableStampInterface` never leave the
    process.
    """

    __slots__ = ("_codecs", "_stamp_types")

    def __init__(
        self,
        stamp_types: Iterable[type[StampInterface]] = (),
        codecs: Sequence[MessageCodecInterface] = (),
    ) -> None:
        """Restore only ``stamp_types`` on decode; convert messages via ``codecs``."""
        self._stamp_types = {t.__name__: t for t in stamp_types}
        self._codecs: tuple[MessageCodecInterface, ...] = tuple(codecs) or default_codecs()

    @override
    def encode(self, envelope: Envelope) -> EncodedEnvelope:
        """Render ``envelope`` as a JSON body plus type and stamp headers.

        Raises:
            MessageDecodingFailedError: If no codec handles the message type.
        """
        message_type = type(envelope.message)
        body = self._codec_for(message_type).encode(envelope.message)
        headers = {TYPE_HEADER: name_of(message_type)}
        headers.update(_encode_stamps(envelope.without_stamps(NonSendableStampInterface).stamps))
        return EncodedEnvelope(body=_dump(body), headers=headers)

    @override
    def decode(self, encoded: EncodedEnvelope) -> Envelope:
        """Rebuild the typed message and its stamps.

        Raises:
            MessageDecodingFailedError: If the body, the type header, or a
                field does not match the registered message type.
        """
        name = encoded.headers.get(TYPE_HEADER)
        if not name:
            raise MessageDecodingFailedError(f"missing {TYPE_HEADER!r} header")
        try:
            message_type = type_for_name(name)
        except UnknownMessageNameError as exc:
            raise MessageDecodingFailedError(str(exc), message_name=name) from exc
        raw = _load(encoded.body, name)
        message = self._codec_for(message_type).decode(message_type, raw)
        return Envelope(message, self._decode_stamps(encoded.headers, name))

    def _codec_for(self, message_type: type) -> MessageCodecInterface:
        for codec in self._codecs:
            if codec.supports(message_type):
                return codec
        raise MessageDecodingFailedError(
            f"no codec handles {message_type.__name__!r}; pass one to JsonSerializer(codecs=...)",
        )

    def _decode_stamps(self, headers: Mapping[str, str], name: str) -> tuple[StampInterface, ...]:
        stamps: list[StampInterface] = []
        for header, value in headers.items():
            if not header.startswith(STAMP_HEADER_PREFIX):
                continue
            stamp_type = self._stamp_types.get(header[len(STAMP_HEADER_PREFIX) :])
            if stamp_type is None:
                continue
            stamps.extend(_decode_stamp_list(stamp_type, value, name))
        return tuple(stamps)


def _encode_stamps(stamps: tuple[StampInterface, ...]) -> dict[str, str]:
    grouped: dict[str, list[object]] = {}
    for stamp in stamps:
        grouped.setdefault(type(stamp).__name__, []).append(
            cast("object", msgspec.to_builtins(stamp))
        )
    return {f"{STAMP_HEADER_PREFIX}{name}": _dump(items) for name, items in grouped.items()}


def _decode_stamp_list(
    stamp_type: type[StampInterface], value: str, name: str
) -> list[StampInterface]:
    raw_list = _load(value, name)
    if not isinstance(raw_list, list):
        raise MessageDecodingFailedError("stamp header must hold a JSON list", name)
    decoded: list[StampInterface] = []
    for raw in raw_list:
        stamp = _convert(stamp_type, cast("JsonValue", raw), name)
        if not isinstance(stamp, StampInterface):
            raise MessageDecodingFailedError(
                f"{stamp_type.__name__} did not decode to a stamp", name
            )
        decoded.append(stamp)
    return decoded


def _dump(value: object) -> str:
    """Render ``value`` as a JSON string."""
    return msgspec.json.encode(value).decode()


def _load(body: str, message_name: str | None = None) -> JsonValue:
    """Parse ``body`` into a JSON-native structure.

    Raises:
        MessageDecodingFailedError: If ``body`` is not valid JSON.
    """
    try:
        return cast("JsonValue", msgspec.json.decode(body))
    except msgspec.DecodeError as exc:
        raise MessageDecodingFailedError(f"not valid JSON: {exc}", message_name) from exc


def _convert(stamp_type: type[StampInterface], raw: JsonValue, name: str) -> object:
    """Rebuild one stamp, reporting a malformed one against its message.

    Raises:
        MessageDecodingFailedError: If ``raw`` does not fit ``stamp_type``.
    """
    try:
        return msgspec.convert(raw, type=stamp_type, strict=True)
    except (msgspec.ValidationError, TypeError) as exc:
        raise MessageDecodingFailedError(str(exc), name) from exc
