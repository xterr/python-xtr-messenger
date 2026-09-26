from __future__ import annotations

import inspect
from typing import Annotated, final, get_type_hints

import pytest
from xtr_dependency_injection import Injected

from tests.support.messages import IngestDocument, ingest_document
from xtr_messenger import Envelope, HandlerDescriptor, HandlerSignatureError

pytestmark = pytest.mark.anyio


@final
class Session:
    """Something a container would build and inject."""


# Held in module globals so get_type_hints resolves them at runtime; deferring the
# imports to TYPE_CHECKING would make the shape check silently stop seeing them.
InjectedSession = Injected[Session]
PlainAnnotatedSession = Annotated[Session, "not a container marker"]


@final
class CallableHandler:
    """A handler that is an object; its annotations live on ``__call__``."""

    async def __call__(self, message: IngestDocument, envelope: Envelope) -> None:
        del message, envelope


@final
class ClassHandler:
    """A handler class, built afresh for every message."""

    async def __call__(self, message: IngestDocument) -> None:
        del message


def test_a_function_handler_takes_only_the_message() -> None:
    async def handle(message: IngestDocument) -> None:
        del message

    descriptor = HandlerDescriptor.of(handle)

    assert descriptor.handler is handle
    assert descriptor.call is handle
    assert descriptor.wants_envelope is False


def test_a_function_handler_can_ask_for_the_envelope() -> None:
    async def handle(message: IngestDocument, envelope: Envelope) -> None:
        del message, envelope

    assert HandlerDescriptor.of(handle).wants_envelope is True


def test_a_bound_method_handler_is_supported() -> None:
    class Service:
        async def handle(self, message: IngestDocument) -> None:
            del message

    method = Service().handle

    descriptor = HandlerDescriptor.of(method)

    assert descriptor.call is method
    assert descriptor.wants_envelope is False


def test_a_callable_object_reads_the_annotations_on_its_call() -> None:
    """Reading them off the instance found nothing; a container-built handler was rejected."""
    handler = CallableHandler()

    descriptor = HandlerDescriptor.of(handler)

    assert descriptor.handler is handler
    assert descriptor.call is handler
    assert descriptor.wants_envelope is True
    assert descriptor.name == "CallableHandler"


def test_a_class_handler_keeps_the_class_but_calls_a_per_message_builder() -> None:
    descriptor = HandlerDescriptor.of(ClassHandler)

    assert descriptor.handler is ClassHandler
    assert descriptor.call is not ClassHandler
    assert descriptor.wants_envelope is False
    assert descriptor.name == "ClassHandler"


def test_the_name_defaults_to_the_handler_qualname() -> None:
    async def handle(message: IngestDocument) -> None:
        del message

    assert HandlerDescriptor.of(handle).name == handle.__qualname__


def test_an_explicit_name_overrides_the_default() -> None:
    async def handle(message: IngestDocument) -> None:
        del message

    assert HandlerDescriptor.of(handle, "custom").name == "custom"


def test_a_second_parameter_that_is_not_an_envelope_is_refused() -> None:
    async def handle(message: IngestDocument, extra: str) -> None:
        del message, extra

    with pytest.raises(HandlerSignatureError, match="annotated Envelope"):
        _ = HandlerDescriptor.of(handle)


def test_a_third_parameter_is_refused() -> None:
    async def handle(message: IngestDocument, envelope: Envelope, extra: str) -> None:
        del message, envelope, extra

    with pytest.raises(HandlerSignatureError) as excinfo:
        _ = HandlerDescriptor.of(handle)

    assert excinfo.value.parameters == ("message", "envelope", "extra")


def test_a_class_that_defines_no_call_is_refused() -> None:
    class NotAHandler:
        pass

    with pytest.raises(HandlerSignatureError):
        _ = HandlerDescriptor.of(NotAHandler)


def test_an_injected_parameter_after_the_message_is_ignored_by_the_shape_check() -> None:
    async def handle(message: IngestDocument, db: InjectedSession) -> None:
        del message, db

    assert HandlerDescriptor.of(handle).wants_envelope is False


def test_injected_parameters_may_follow_the_message_and_the_envelope() -> None:
    async def handle(message: IngestDocument, envelope: Envelope, db: InjectedSession) -> None:
        del message, envelope, db

    assert HandlerDescriptor.of(handle).wants_envelope is True


def test_an_injected_parameter_before_the_message_is_refused() -> None:
    """The bus passes the message by position; anything before it would take its place."""

    async def handle(db: InjectedSession, message: IngestDocument) -> None:
        del db, message

    with pytest.raises(HandlerSignatureError):
        _ = HandlerDescriptor.of(handle)


def test_a_plain_annotated_parameter_is_not_taken_for_an_injected_one() -> None:
    async def handle(message: IngestDocument, extra: PlainAnnotatedSession) -> None:
        del message, extra

    with pytest.raises(HandlerSignatureError, match="annotated Envelope"):
        _ = HandlerDescriptor.of(handle)


def test_the_declared_function_is_never_mutated() -> None:
    """Registration reads the signature; it does not rewrite it."""

    async def handle(message: IngestDocument, db: InjectedSession) -> None:
        del message, db

    signature_before = inspect.signature(handle)
    hints_before = get_type_hints(handle, include_extras=True)

    _ = HandlerDescriptor.of(handle)

    assert inspect.signature(handle) == signature_before
    assert get_type_hints(handle, include_extras=True) == hints_before


def test_unresolvable_annotations_do_not_crash_registration() -> None:
    """``get_type_hints`` cannot see a name local to the enclosing scope; that is fine."""

    class LocalOnly:
        pass

    async def handle(message: LocalOnly) -> None:
        del message

    assert HandlerDescriptor.of(handle).wants_envelope is False


async def test_invoke_passes_only_the_message_when_the_envelope_is_not_wanted() -> None:
    received: list[object] = []

    async def handle(message: IngestDocument) -> None:
        received.append(message)

    envelope = Envelope(ingest_document())

    await HandlerDescriptor.of(handle).invoke(envelope)

    assert received == [envelope.message]


async def test_invoke_passes_the_envelope_when_it_is_wanted() -> None:
    received: list[tuple[object, Envelope]] = []

    async def handle(message: IngestDocument, envelope: Envelope) -> None:
        received.append((message, envelope))

    envelope = Envelope(ingest_document())

    await HandlerDescriptor.of(handle).invoke(envelope)

    assert received == [(envelope.message, envelope)]


async def test_a_class_handler_is_built_once_with_no_arguments_and_reused() -> None:
    """A handler is a service: one per message would cost a construction each."""
    built: list[object] = []
    called: list[object] = []

    @final
    class Handler:
        def __init__(self) -> None:
            built.append(self)

        async def __call__(self, message: IngestDocument) -> None:
            del message
            called.append(self)

    descriptor = HandlerDescriptor.of(Handler)
    envelope = Envelope(ingest_document())

    await descriptor.invoke(envelope)
    await descriptor.invoke(envelope)

    assert len(built) == 1
    assert called == [built[0], built[0]]


def test_declaring_a_class_handler_builds_nothing() -> None:
    """Built on its first message, not at import, where its needs may not exist yet."""
    built: list[object] = []

    @final
    class Handler:
        def __init__(self) -> None:
            built.append(self)

        async def __call__(self, message: IngestDocument) -> None:
            del message

    _ = HandlerDescriptor.of(Handler)

    assert built == []
