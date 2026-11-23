"""A message bus built from envelopes, stamps and a middleware chain.

Dispatch wraps a message in an :class:`Envelope` and walks it through a
middleware chain. MiddlewareInterface records what it did by appending a
:class:`StampInterface`; routing sends the envelope to one or more named transports.

A message crossing a transport is written and read back with xtr-serializer,
so it arrives exactly as it left. A transport that needs a broker library is a
package of its own, found by its DSN scheme once installed — RabbitMQ is
xtr-amqp-messenger, the ``amqp`` extra.
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
    InvalidCommandInputError,
    InvalidMiddlewareArgumentsError,
    InvalidTransportOptionError,
    MessageBusError,
    MessageDecodingFailedError,
    MessageEncodingFailedError,
    MissingSerializerError,
    MixedDsnError,
    NoFailureTransportError,
    NoHandlerForMessageError,
    NoSenderForMessageError,
    NotConsumableError,
    UnknownMessageNameError,
    UnknownMiddlewareError,
    UnknownTransportError,
    UnknownTransportOptionError,
    UnsupportedStampError,
    UnsupportedTransportOperationError,
)
from .failure import FailedMessageFilter, FailedMessageRepository, FailureTransportSender
from .failure_transport_worker_providing_interface import (
    FailureTransportWorkerProvidingInterface,
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
    SentToFailureTransportStamp,
    StampInterface,
    TransportMessageIdStamp,
    TransportNamesStamp,
)
from .stamp_registry import declared_stamps, register_stamp, stamp_type_for
from .transport.in_memory import InMemoryTransport, InMemoryTransportFactory
from .transport.receiver.listable_receiver_interface import ListableReceiverInterface
from .transport.receiver.message_count_aware_interface import MessageCountAwareInterface
from .transport.receiver.receiver_interface import ReceiverInterface
from .transport.receiver.retrying_receiver_interface import RetryingReceiverInterface
from .transport.receiver.single_message_receiver import SingleMessageReceiver
from .transport.sender import SenderInterface, SendersLocator, SendersLocatorInterface
from .transport.serialization import (
    EncodedEnvelope,
    JsonSerializer,
    SerializerInterface,
)
from .transport.setupable_transport_interface import SetupableTransportInterface
from .transport.sync import SyncTransport, SyncTransportFactory
from .transport.transport_config import TransportConfig
from .transport.transport_factory import TransportFactory
from .transport.transport_factory_discovery import default_factories
from .transport.transport_factory_interface import (
    TransportFactoryInterface,
    UnsupportedDsnError,
)
from .transport.transport_interface import TransportInterface
from .transport.transport_locator import TransportLocator
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
    "DelayStamp",
    "DelayedMessageHandlingError",
    "DispatchAfterCurrentBusMiddleware",
    "DispatchAfterCurrentBusStamp",
    "Dsn",
    "EncodedEnvelope",
    "Envelope",
    "ErrorDetailsStamp",
    "FailedMessageFilter",
    "FailedMessageRepository",
    "FailureTransportSender",
    "FailureTransportWorkerProvidingInterface",
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
    "InvalidCommandInputError",
    "InvalidDsnError",
    "InvalidMiddlewareArgumentsError",
    "InvalidTransportOptionError",
    "JsonSerializer",
    "ListableReceiverInterface",
    "LoggingMiddleware",
    "MessageBus",
    "MessageBusConfig",
    "MessageBusError",
    "MessageBusFactory",
    "MessageBusInterface",
    "MessageCountAwareInterface",
    "MessageDecodingFailedError",
    "MessageEncodingFailedError",
    "MiddlewareInterface",
    "MissingSerializerError",
    "MixedDsnError",
    "NoFailureTransportError",
    "NoHandlerForMessageError",
    "NoSenderForMessageError",
    "NonSendableStampInterface",
    "NotConsumableError",
    "PublisherClosingInterface",
    "ReceivedStamp",
    "ReceiverInterface",
    "RedeliveryStamp",
    "RedispatchMessage",
    "RetryingReceiverInterface",
    "SendMessageMiddleware",
    "SenderInterface",
    "SendersLocator",
    "SendersLocatorInterface",
    "SentStamp",
    "SentToFailureTransportStamp",
    "SerializerInterface",
    "SetupableTransportInterface",
    "SingleMessageReceiver",
    "StackInterface",
    "StackMiddleware",
    "StampInterface",
    "SyncTransport",
    "SyncTransportFactory",
    "TransportConfig",
    "TransportFactory",
    "TransportFactoryInterface",
    "TransportInterface",
    "TransportLocator",
    "TransportMessageIdStamp",
    "TransportNamesStamp",
    "UnknownMessageNameError",
    "UnknownMiddlewareError",
    "UnknownTransportError",
    "UnknownTransportOptionError",
    "UnsupportedDsnError",
    "UnsupportedStampError",
    "UnsupportedTransportOperationError",
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
    "default_factories",
    "default_registry",
    "name_of",
    "register_message",
    "register_stamp",
    "stamp_type_for",
    "transports_of",
    "type_for_name",
]
