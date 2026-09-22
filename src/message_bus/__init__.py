"""A message bus built from envelopes, stamps and a middleware chain.

Dispatch wraps a message in an :class:`Envelope` and walks it through a
middleware chain. MiddlewareInterface records what it did by appending a
:class:`StampInterface`; routing sends the envelope to one or more named transports.

The core has no third-party dependencies. Transports that need one live
behind an extra — see :mod:`message_bus.bridge.amqp`.
"""

from .decorator import as_message, as_message_handler
from .dsn import Dsn, InvalidDsnError
from .envelope import Envelope
from .exception import (
    HandlerSignatureError,
    MessageBusError,
    MessageDecodingFailedError,
    MissingTaskRouteError,
    MixedDsnError,
    NoHandlerForMessageError,
    NoSenderForMessageError,
    NotConsumableError,
    UnknownMessageNameError,
    UnknownTransportError,
    UnknownTransportNameError,
    UnknownTransportOptionError,
)
from .handler import (
    Handler,
    HandlerDescriptor,
    HandlersLocator,
    HandlersLocatorInterface,
    default_registry,
)
from .message_bus import MessageBus
from .message_bus_config import MessageBusConfig
from .message_bus_factory import MessageBusFactory
from .message_bus_interface import MessageBusInterface
from .message_registry import name_of, transports_of, type_for_name
from .middleware import (
    HandleMessageMiddleware,
    LoggerInterface,
    LoggingMiddleware,
    MiddlewareInterface,
    SendMessageMiddleware,
    StackInterface,
    StackMiddleware,
)
from .stamp import (
    AckReceiptStamp,
    BusNameStamp,
    DelayStamp,
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

__version__ = "0.1.0"

__all__ = [
    "AckReceiptStamp",
    "BusNameStamp",
    "DataclassCodec",
    "DelayStamp",
    "Dsn",
    "EncodedEnvelope",
    "Envelope",
    "ErrorDetailsStamp",
    "HandleMessageMiddleware",
    "HandledStamp",
    "Handler",
    "HandlerDescriptor",
    "HandlerSignatureError",
    "HandlersLocator",
    "HandlersLocatorInterface",
    "InMemoryTransport",
    "InMemoryTransportFactory",
    "InvalidDsnError",
    "JsonSerializer",
    "LoggerInterface",
    "LoggingMiddleware",
    "MessageBus",
    "MessageBusConfig",
    "MessageBusError",
    "MessageBusFactory",
    "MessageBusInterface",
    "MessageCodecInterface",
    "MessageDecodingFailedError",
    "MiddlewareInterface",
    "MissingTaskRouteError",
    "MixedDsnError",
    "NoHandlerForMessageError",
    "NoSenderForMessageError",
    "NonSendableStampInterface",
    "NotConsumableError",
    "ReceivedStamp",
    "ReceiverInterface",
    "RedeliveryStamp",
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
    "UnknownTransportError",
    "UnknownTransportNameError",
    "UnknownTransportOptionError",
    "UnsupportedDsnError",
    "Worker",
    "WorkerFactory",
    "WorkerInterface",
    "WorkerProvidingInterface",
    "__version__",
    "as_message",
    "as_message_handler",
    "default_codecs",
    "default_factories",
    "default_registry",
    "name_of",
    "transports_of",
    "type_for_name",
]
