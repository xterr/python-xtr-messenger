"""Turning envelopes into something a transport can carry, and back.

An encoded envelope is a ``body`` string plus a flat ``headers`` mapping. The
message's stable name travels in the ``type`` header, and each stamp class
gets its own ``X-Message-Stamp-<Name>`` header.

Owning this boundary is what lets a schema drift surface as
:class:`~xtr_messenger.exception.MessageDecodingFailedError` instead of a
wrongly shaped object arriving deep inside a handler.
"""

from __future__ import annotations

from types import GenericAlias
from typing import TYPE_CHECKING, cast, final

from typing_extensions import override
from xtr_serializer import SerializerError

from xtr_messenger.envelope import Envelope
from xtr_messenger.exception import (
    MessageDecodingFailedError,
    MessageEncodingFailedError,
    UnknownMessageNameError,
)
from xtr_messenger.message_registry import name_of, type_for_name
from xtr_messenger.stamp import (
    NonSendableStampInterface,
    StampInterface,
)
from xtr_messenger.stamp_registry import stamp_type_for

from .encoded_envelope import EncodedEnvelope
from .serializer_interface import SerializerInterface

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    import xtr_serializer

__all__ = ["STAMP_HEADER_PREFIX", "TYPE_HEADER", "JsonSerializer"]

TYPE_HEADER = "type"
STAMP_HEADER_PREFIX = "X-Message-Stamp-"

_FORMAT = "json"


@final
class JsonSerializer(SerializerInterface):
    """Encodes messages and their stamps as JSON, exactly.

    The body is written by ``serializer`` as the message's own class, and read
    back as the class its ``type`` header names, so it arrives as it left:
    nested dataclasses and models, decimals, enums, a ``ZonedDateTime``, a
    subclass in a field typed with its base when the base declares a
    hierarchy. A message that could not make the trip exactly is refused when
    it is encoded. Every stamp is written the same way, so an application's
    stamp keeps its values too.

    Decoding is an allow-list: the ``type`` header resolves only to a declared
    message, and a stamp header names a class this process must import, so an
    unrecognised one is dropped rather than resolved. The stamp list defaults
    to the stamps the library ships plus every stamp declared with
    :func:`~xtr_messenger.decorator.as_stamp` — declare a custom stamp, or pass
    ``stamp_types``, for it to survive the trip.

    What arrives is untrusted, so a decoding error says what did not fit and
    where, never what the data held.

    Stamps inheriting
    :class:`~xtr_messenger.stamp.NonSendableStampInterface` never leave the
    process, so they are never written and never restored.
    """

    __slots__ = ("_serializer", "_stamp_types")

    def __init__(
        self,
        serializer: xtr_serializer.SerializerInterface,
        stamp_types: Iterable[type[StampInterface]] | None = None,
    ) -> None:
        """Write bodies and stamps with ``serializer``; restore ``stamp_types`` on decode.

        Omitting ``stamp_types`` restores the stamps the library ships and
        every stamp declared with :func:`~xtr_messenger.decorator.as_stamp`
        — including one declared after this serializer was built. Pass an
        explicit iterable to restore exactly those, or ``()`` to restore none
        at all.
        """
        self._serializer = serializer
        self._stamp_types: dict[str, type[StampInterface]] | None = (
            None if stamp_types is None else {t.__name__: t for t in stamp_types}
        )

    @override
    def encode(self, envelope: Envelope) -> EncodedEnvelope:
        """Render ``envelope`` as a JSON body plus type and stamp headers.

        Raises:
            MessageEncodingFailedError: If the message or a stamp holds a
                value that would not read back exactly.
        """
        message_type = type(envelope.message)
        name = name_of(message_type)
        try:
            body = self._serializer.serialize(envelope.message, _FORMAT, type_=message_type)
            headers = {TYPE_HEADER: name}
            headers.update(self._encode_stamps(envelope.without_stamps(NonSendableStampInterface)))
        except SerializerError as error:
            raise MessageEncodingFailedError(str(error), name) from error
        return EncodedEnvelope(body=body, headers=headers)

    @override
    def decode(self, encoded: EncodedEnvelope) -> Envelope:
        """Rebuild the typed message and its stamps.

        Raises:
            MessageDecodingFailedError: If the type header is missing or names
                no declared message, or the body or a stamp header does not
                fit its class.
        """
        name = encoded.headers.get(TYPE_HEADER)
        if not name:
            raise MessageDecodingFailedError(f"missing {TYPE_HEADER!r} header")
        message_type = _declared(name)
        if message_type is None:
            # Raised outside the lookup's handler: its error repeats the name, which is wire data.
            raise MessageDecodingFailedError(
                f"the {TYPE_HEADER!r} header names no declared message"
            )
        try:
            message = self._serializer.deserialize(encoded.body, message_type, _FORMAT)
        except SerializerError as error:
            raise MessageDecodingFailedError(str(error), name) from error
        return Envelope(message, self._decode_stamps(encoded.headers, name))

    def _encode_stamps(self, envelope: Envelope) -> dict[str, str]:
        grouped: dict[str, list[StampInterface]] = {}
        for stamp in envelope.stamps:
            grouped.setdefault(type(stamp).__name__, []).append(stamp)
        return {
            f"{STAMP_HEADER_PREFIX}{stamp_name}": self._serializer.serialize(
                stamps, _FORMAT, type_=_list_of(type(stamps[0]))
            )
            for stamp_name, stamps in grouped.items()
        }

    def _decode_stamps(self, headers: Mapping[str, str], name: str) -> tuple[StampInterface, ...]:
        stamps: list[StampInterface] = []
        for header, value in headers.items():
            if not header.startswith(STAMP_HEADER_PREFIX):
                continue
            stamp_type = self._stamp_type_for(header[len(STAMP_HEADER_PREFIX) :])
            if stamp_type is None:
                continue
            try:
                decoded = self._serializer.deserialize(value, _list_of(stamp_type), _FORMAT)
            except SerializerError as error:
                reason = f"{stamp_type.__name__} header: {error}"
                raise MessageDecodingFailedError(reason, name) from error
            # Read as exactly that list type: the serializer refuses anything else.
            stamps.extend(cast("list[StampInterface]", decoded))
        return tuple(stamps)

    def _stamp_type_for(self, name: str) -> type[StampInterface] | None:
        if self._stamp_types is None:
            return stamp_type_for(name)
        return self._stamp_types.get(name)


def _list_of(stamp_type: type[StampInterface]) -> GenericAlias:
    """Return ``list[stamp_type]``, the type a stamp header is declared with.

    Built as a value: the stamp class is only known at runtime.
    """
    return GenericAlias(list, (stamp_type,))


def _declared(name: str) -> type[object] | None:
    """Return the message declared under ``name``, or ``None`` when there is none."""
    try:
        return type_for_name(name)
    except UnknownMessageNameError:
        return None
