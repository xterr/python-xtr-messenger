"""Envelope stamps — the immutable metadata markers middleware attach.

A stamp is the bus's only side-channel: middleware records what it did by
appending a stamp to the :class:`~message_bus.envelope.Envelope` rather than
mutating the message or widening a return type. Upstream code reads the
result back by stamp type.

Stamps that describe purely in-process facts subclass
:class:`NonSendableStampInterface` and are stripped before a serializer puts the
envelope on the wire.
"""

from .ack_receipt_stamp import AckReceiptStamp
from .delay_stamp import DelayStamp
from .error_details_stamp import ErrorDetailsStamp
from .handled_stamp import HandledStamp
from .non_sendable_stamp_interface import NonSendableStampInterface
from .received_stamp import ReceivedStamp
from .redelivery_stamp import RedeliveryStamp
from .sent_stamp import SentStamp
from .stamp_interface import StampInterface
from .transport_message_id_stamp import TransportMessageIdStamp
from .transport_names_stamp import TransportNamesStamp

__all__ = [
    "DEFAULT_STAMP_TYPES",
    "AckReceiptStamp",
    "DelayStamp",
    "ErrorDetailsStamp",
    "HandledStamp",
    "NonSendableStampInterface",
    "ReceivedStamp",
    "RedeliveryStamp",
    "SentStamp",
    "StampInterface",
    "TransportMessageIdStamp",
    "TransportNamesStamp",
]


#: The stamps a serializer restores on decode when told nothing else.
#:
#: Encoding already writes every sendable stamp; this is the other half, and
#: leaving it empty meant a producer's stamps were written to the wire and
#: silently discarded by the consumer. Decoding stays an allow-list — a stamp
#: names a class this process must import, so an unknown name is dropped
#: rather than resolved — but the list defaults to what the library ships.
DEFAULT_STAMP_TYPES: tuple[type[StampInterface], ...] = (
    DelayStamp,
    ErrorDetailsStamp,
    RedeliveryStamp,
    TransportMessageIdStamp,
    TransportNamesStamp,
)
