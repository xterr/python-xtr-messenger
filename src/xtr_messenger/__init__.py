"""A message bus built from envelopes, stamps and a middleware chain.

Dispatch wraps a message in an :class:`Envelope` and walks it through a
middleware chain. MiddlewareInterface records what it did by appending a
:class:`StampInterface`; routing sends the envelope to one or more named transports.

The core has no third-party dependencies. Transports that need one live
behind an extra — see :mod:`xtr_messenger.bridge.amqp`.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from .decorator import as_message, as_message_handler, as_middleware, as_stamp
from .dsn import Dsn, InvalidDsnError
from .envelope import Envelope
from .exception import (
    DelayedMessageHandlingError,
    HandlersFailedError,
    HandlerSignatureError,
    IncompatibleReceiversError,
    InvalidMiddlewareArgumentsError,
    InvalidTransportOptionError,
    MessageBusError,
    MessageDecodingFailedError,
    MessageEncodingFailedError,
    MixedDsnError,
    NoHandlerForMessageError,
    NoSenderForMessageError,
    NotConsumableError,
    UnknownMessageNameError,
    UnknownMiddlewareError,
    UnknownTransportError,
    UnknownTransportOptionError,
    UnsupportedStampError,
)
from .handler import (
    Handler,
    HandlerDescriptor,
    HandlersLocator,
    HandlersLocatorInterface,
    default_registry,
)
from .message import RedispatchMessage
from .message_bus import MessageBus
from .message_bus_config import MessageBusConfig
from .message_bus_factory import MessageBusFactory
from .message_bus_interface import MessageBusInterface
from .message_registry import (
    declared_names,
    name_of,
    register_message,
    transports_of,
    type_for_name,
)
from .middleware import (
    DispatchAfterCurrentBusMiddleware,
    HandleMessageMiddleware,
    LoggingMiddleware,
    MiddlewareInterface,
    SendMessageMiddleware,
    StackInterface,
    StackMiddleware,
)
from .publisher_closing_interface import PublisherClosingInterface
from .stamp import (
    AckReceiptStamp,
    DelayStamp,
    DispatchAfterCurrentBusStamp,
    ErrorDetailsStamp,
    HandledStamp,
    NonSendableStampInterface,
    ReceivedStamp,
    RedeliveryStamp,
    SentStamp,
    StampInterface,
    TransportMessageIdStamp,
    TransportNamesStamp,
)
from .stamp_registry import declared_stamps, register_stamp, stamp_type_for
from .transport.in_memory import InMemoryTransport, InMemoryTransportFactory
from .transport.receiver.receiver_interface import ReceiverInterface
from .transport.sender import SenderInterface, SendersLocator, SendersLocatorInterface
from .transport.serialization import (
    EncodedEnvelope,
    JsonSerializer,
    SerializerInterface,
    default_codecs,
)
from .transport.serialization.codec import DataclassCodec, MessageCodecInterface
from .transport.sync import SyncTransport, SyncTransportFactory
from .transport.transport_config import TransportConfig
from .transport.transport_factory import TransportFactory
from .transport.transport_factory_discovery import default_factories
from .transport.transport_factory_interface import (
    TransportFactoryInterface,
    UnsupportedDsnError,
)
from .transport.transport_interface import TransportInterface
from .worker import Worker
from .worker_factory import WorkerFactory
from .worker_interface import WorkerInterface
from .worker_providing_interface import WorkerProvidingInterface

try:
    __version__ = version("xtr-messenger")
except PackageNotFoundError:  # pragma: no cover
    # Running from a source tree with no installed metadata to read; having no
    # version is better than refusing to import.
    __version__ = "0+unknown"

__all__ = [
    "AckReceiptStamp",
    "DataclassCodec",
    "DelayStamp",
    "DelayedMessageHandlingError",
    "DispatchAfterCurrentBusMiddleware",
    "DispatchAfterCurrentBusStamp",
    "Dsn",
    "EncodedEnvelope",
    "Envelope",
    "ErrorDetailsStamp",
    "HandleMessageMiddleware",
    "HandledStamp",
    "Handler",
    "HandlerDescriptor",
    "HandlerSignatureError",
    "HandlersFailedError",
    "HandlersLocator",
    "HandlersLocatorInterface",
    "InMemoryTransport",
    "InMemoryTransportFactory",
    "IncompatibleReceiversError",
    "InvalidDsnError",
    "InvalidMiddlewareArgumentsError",
    "InvalidTransportOptionError",
    "JsonSerializer",
    "LoggingMiddleware",
    "MessageBus",
    "MessageBusConfig",
    "MessageBusError",
    "MessageBusFactory",
    "MessageBusInterface",
    "MessageCodecInterface",
    "MessageDecodingFailedError",
    "MessageEncodingFailedError",
    "MiddlewareInterface",
    "MixedDsnError",
    "NoHandlerForMessageError",
    "NoSenderForMessageError",
    "NonSendableStampInterface",
    "NotConsumableError",
    "PublisherClosingInterface",
    "ReceivedStamp",
    "ReceiverInterface",
    "RedeliveryStamp",
    "RedispatchMessage",
    "SendMessageMiddleware",
    "SenderInterface",
    "SendersLocator",
    "SendersLocatorInterface",
    "SentStamp",
    "SerializerInterface",
    "StackInterface",
    "StackMiddleware",
    "StampInterface",
    "SyncTransport",
    "SyncTransportFactory",
    "TransportConfig",
    "TransportFactory",
    "TransportFactoryInterface",
    "TransportInterface",
    "TransportMessageIdStamp",
    "TransportNamesStamp",
    "UnknownMessageNameError",
    "UnknownMiddlewareError",
    "UnknownTransportError",
    "UnknownTransportOptionError",
    "UnsupportedDsnError",
    "UnsupportedStampError",
    "Worker",
    "WorkerFactory",
    "WorkerInterface",
    "WorkerProvidingInterface",
    "__version__",
    "as_message",
    "as_message_handler",
    "as_middleware",
    "as_stamp",
    "declared_names",
    "declared_stamps",
    "default_codecs",
    "default_factories",
    "default_registry",
    "name_of",
    "register_message",
    "register_stamp",
    "stamp_type_for",
    "transports_of",
    "type_for_name",
]
