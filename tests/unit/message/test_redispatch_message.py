"""A message asking for another envelope to be dispatched again."""

from __future__ import annotations

from dataclasses import dataclass

from typing_extensions import override

from tests.support.messages import ingest_document
from xtr_messenger import Envelope, RedispatchMessage


@dataclass(frozen=True, slots=True)
class Described:
    """A message with a string form of its own."""

    text: str

    @override
    def __str__(self) -> str:
        return f"described {self.text}"


def test_one_transport_name_becomes_a_tuple_of_one() -> None:
    assert RedispatchMessage("m", "jobs").transport_names == ("jobs",)


def test_empty_names_mean_no_override() -> None:
    assert RedispatchMessage("m", ["", "jobs", ""]).transport_names == ("jobs",)
    assert RedispatchMessage("m", "").transport_names == ()


def test_the_message_is_read_through_an_envelope_or_as_given() -> None:
    message = ingest_document()

    assert RedispatchMessage(Envelope(message)).message is message
    assert RedispatchMessage(message).message is message


def test_its_string_form_is_the_message_type_when_the_message_has_none() -> None:
    assert str(RedispatchMessage(ingest_document())) == "IngestDocument"


def test_its_string_form_uses_the_message_s_own_and_names_the_transports() -> None:
    redispatch = RedispatchMessage(Envelope(Described("x")), ("high", "audit"))

    assert str(redispatch) == "described x via high, audit"


def test_it_is_a_value() -> None:
    assert RedispatchMessage("m", "a") == RedispatchMessage("m", ("a",))
