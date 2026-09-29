"""Every error this library raises.

All of them derive from :class:`MessageBusError`, so one ``except`` catches
anything the bus can go wrong with, and a narrower one handles a single
cause. Each carries the data a caller needs as typed attributes rather than
forcing a message to be parsed.
"""

from __future__ import annotations

from .handler_signature_error import HandlerSignatureError
from .handlers_failed_error import HandlersFailedError
from .incompatible_receivers_error import IncompatibleReceiversError
from .invalid_dsn_error import InvalidDsnError
from .invalid_middleware_arguments_error import InvalidMiddlewareArgumentsError
from .invalid_transport_option_error import InvalidTransportOptionError
from .message_bus_error import MessageBusError
from .message_decoding_failed_error import MessageDecodingFailedError
from .message_encoding_failed_error import MessageEncodingFailedError
from .mixed_dsn_error import MixedDsnError
from .no_handler_for_message_error import NoHandlerForMessageError
from .no_sender_for_message_error import NoSenderForMessageError
from .not_consumable_error import NotConsumableError
from .unknown_message_name_error import UnknownMessageNameError
from .unknown_middleware_error import UnknownMiddlewareError
from .unknown_transport_error import UnknownTransportError
from .unknown_transport_option_error import UnknownTransportOptionError
from .unsupported_dsn_error import UnsupportedDsnError

__all__ = [
    "HandlerSignatureError",
    "HandlersFailedError",
    "IncompatibleReceiversError",
    "InvalidDsnError",
    "InvalidMiddlewareArgumentsError",
    "InvalidTransportOptionError",
    "MessageBusError",
    "MessageDecodingFailedError",
    "MessageEncodingFailedError",
    "MixedDsnError",
    "NoHandlerForMessageError",
    "NoSenderForMessageError",
    "NotConsumableError",
    "UnknownMessageNameError",
    "UnknownMiddlewareError",
    "UnknownTransportError",
    "UnknownTransportOptionError",
    "UnsupportedDsnError",
]
