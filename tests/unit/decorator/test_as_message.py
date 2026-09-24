from __future__ import annotations

from dataclasses import dataclass

from message_bus import as_message, name_of, transports_of


def test_a_bare_decoration_returns_the_class_unchanged() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert as_message(Message) is Message


def test_a_bare_decoration_records_the_default_name() -> None:
    @as_message
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert name_of(Message) == f"{Message.__module__}:{Message.__qualname__}"


def test_empty_parentheses_return_the_class_unchanged() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert as_message()(Message) is Message


def test_empty_parentheses_record_the_default_name() -> None:
    @as_message()
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert name_of(Message) == f"{Message.__module__}:{Message.__qualname__}"


def test_a_name_only_declaration_pins_the_wire_name() -> None:
    @as_message(name="test.unit.decorator.as_message.name_only.v1")
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert name_of(Message) == "test.unit.decorator.as_message.name_only.v1"
    assert transports_of(Message) == ()


def test_a_transport_string_is_recorded_as_a_single_default() -> None:
    @as_message(transport="jobs")
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert transports_of(Message) == ("jobs",)


def test_a_transport_sequence_is_recorded_in_order() -> None:
    @as_message(transport=["high", "low"])
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert transports_of(Message) == ("high", "low")


def test_a_name_and_transport_can_be_declared_together() -> None:
    @as_message(name="test.unit.decorator.as_message.both.v1", transport="jobs")
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert name_of(Message) == "test.unit.decorator.as_message.both.v1"
    assert transports_of(Message) == ("jobs",)


def test_a_keyword_declaration_returns_the_class_unchanged() -> None:
    @dataclass(frozen=True, slots=True)
    class Message:
        pass

    assert as_message(name="test.unit.decorator.as_message.identity.v1")(Message) is Message
