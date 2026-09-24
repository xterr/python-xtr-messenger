from __future__ import annotations

from dataclasses import dataclass

import pytest

from message_bus import (
    MessageBusError,
    UnknownMessageNameError,
    as_message,
    name_of,
    transports_of,
    type_for_name,
)
from message_bus.message_registry import declared_names, register_message
from tests.support.messages import UndeclaredMessage


class Outer:
    class Inner:
        """A nested class, reachable only by walking the qualname part by part."""


def test_an_explicit_name_is_recorded() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    register_message(Message, "test.unit.message_registry.explicit.v1")

    assert name_of(Message) == "test.unit.message_registry.explicit.v1"


def test_a_message_declared_without_a_name_records_its_default_name() -> None:
    @as_message
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert name_of(Message) == f"{Message.__module__}:{Message.__qualname__}"
    assert name_of(Message) in declared_names()


def test_a_declared_name_appears_among_the_declared_names() -> None:
    """``declared_names()`` is process-wide, so membership is the only safe assertion."""

    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    register_message(Message, "test.unit.message_registry.declared.v1")

    assert "test.unit.message_registry.declared.v1" in declared_names()


def test_name_of_falls_back_to_the_default_for_an_undeclared_class() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert name_of(Message) == f"{Message.__module__}:{Message.__qualname__}"


def test_a_name_claimed_by_another_class_is_refused() -> None:
    @dataclass(frozen=True, slots=True)
    class First:
        pass

    @dataclass(frozen=True, slots=True)
    class Second:
        pass

    register_message(First, "test.unit.message_registry.claimed.v1")

    with pytest.raises(MessageBusError):
        register_message(Second, "test.unit.message_registry.claimed.v1")


def test_the_same_class_may_register_the_same_name_again() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    register_message(Message, "test.unit.message_registry.repeat.v1")
    register_message(Message, "test.unit.message_registry.repeat.v1")

    assert name_of(Message) == "test.unit.message_registry.repeat.v1"


def test_registering_again_without_a_name_keeps_the_pinned_name() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    register_message(Message, "test.unit.message_registry.pinned.v1")
    register_message(Message)

    assert name_of(Message) == "test.unit.message_registry.pinned.v1"


def test_transports_of_is_empty_when_nothing_declares_one() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert transports_of(Message) == ()


def test_transports_of_walks_the_bases() -> None:
    """A subclass inherits the transport its base declared."""

    @as_message(name="test.unit.message_registry.base.v1", transport="declared")
    @dataclass(frozen=True, slots=True)
    class Base:
        pass

    class Derived(Base):
        pass

    assert transports_of(Derived) == ("declared",)


def test_type_for_name_resolves_a_registered_name() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    register_message(Message, "test.unit.message_registry.resolve.v1")

    assert type_for_name("test.unit.message_registry.resolve.v1") is Message


def test_type_for_name_falls_back_to_importing_module_qualname() -> None:
    """An undeclared class still resolves through its ``module:QualName`` import path."""
    assert type_for_name("tests.support.messages:UndeclaredMessage") is UndeclaredMessage


def test_type_for_name_resolves_a_nested_qualname() -> None:
    assert type_for_name(f"{Outer.Inner.__module__}:{Outer.Inner.__qualname__}") is Outer.Inner


def test_a_name_without_a_colon_cannot_be_resolved() -> None:
    with pytest.raises(UnknownMessageNameError):
        _ = type_for_name("test.unit.message_registry.no_colon")


def test_a_name_in_an_unimportable_module_cannot_be_resolved() -> None:
    with pytest.raises(UnknownMessageNameError):
        _ = type_for_name("message_bus.does_not_exist:Thing")


def test_a_name_whose_attribute_is_missing_cannot_be_resolved() -> None:
    with pytest.raises(UnknownMessageNameError):
        _ = type_for_name("tests.support.messages:NoSuchClass")


def test_a_name_resolving_to_a_non_type_cannot_be_resolved() -> None:
    """``ingest_document`` is a function, not a message class."""
    with pytest.raises(UnknownMessageNameError):
        _ = type_for_name("tests.support.messages:ingest_document")
