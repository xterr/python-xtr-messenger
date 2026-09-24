from __future__ import annotations

from dataclasses import dataclass

from message_bus import HandlersLocator, as_message, as_message_handler, default_registry


@as_message(name="test.unit.decorator.as_message_handler.local.v1")
@dataclass(frozen=True, slots=True)
class LocalMessage:
    pass


@as_message(name="test.unit.decorator.as_message_handler.default.v1")
@dataclass(frozen=True, slots=True)
class DefaultRegistryMessage:
    """Its own name, so touching the default registry cannot clash with another test."""


def test_a_handler_is_registered_into_the_given_locator() -> None:
    registry = HandlersLocator()

    @as_message_handler(LocalMessage, registry)
    async def handle(message: LocalMessage) -> None:
        del message

    assert registry.message_types() == (LocalMessage,)


def test_the_decorator_returns_the_function_untouched() -> None:
    registry = HandlersLocator()

    async def handle(message: LocalMessage) -> None:
        del message

    assert as_message_handler(LocalMessage, registry)(handle) is handle


def test_a_handler_without_a_registry_lands_in_the_default_one() -> None:
    """The one test that touches the process-wide default registry, with its own message."""

    @as_message_handler(DefaultRegistryMessage)
    async def handle(message: DefaultRegistryMessage) -> None:
        del message

    assert DefaultRegistryMessage in default_registry().message_types()


def test_a_class_can_be_declared_as_a_handler() -> None:
    registry = HandlersLocator()

    @as_message_handler(LocalMessage, registry)
    class Handle:
        async def __call__(self, message: LocalMessage) -> None:
            del message

    assert registry.handlers_for(LocalMessage)[0].handler is Handle
