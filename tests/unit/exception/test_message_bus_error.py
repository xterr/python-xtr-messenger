from __future__ import annotations

import pytest

import xtr_messenger
from xtr_messenger import (
    DelayedMessageHandlingError,
    HandlersFailedError,
    HandlerSignatureError,
    IncompatibleReceiversError,
    InvalidDsnError,
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
    UnsupportedDsnError,
    UnsupportedStampError,
    exception,
)

EXPORTED_EXCEPTIONS: tuple[type, ...] = (
    DelayedMessageHandlingError,
    HandlerSignatureError,
    HandlersFailedError,
    IncompatibleReceiversError,
    InvalidDsnError,
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
    UnsupportedDsnError,
    UnsupportedStampError,
)


class Sample:
    pass


def test_every_error_the_library_raises_is_catchable_from_the_root() -> None:
    """Two were once reachable only through a different import than the rest."""
    unreachable = [name for name in exception.__all__ if name not in xtr_messenger.__all__]

    assert unreachable == []


def test_the_explicit_list_matches_every_exported_exception() -> None:
    assert {exc.__name__ for exc in EXPORTED_EXCEPTIONS} == set(exception.__all__)


def test_every_exported_exception_derives_from_the_root() -> None:
    for exc in EXPORTED_EXCEPTIONS:
        assert issubclass(exc, MessageBusError)


def test_a_raised_subclass_is_caught_as_the_root() -> None:
    with pytest.raises(MessageBusError):
        raise InvalidDsnError("just-a-host")


def test_handler_signature_error_carries_the_handler_and_parameters() -> None:
    error = HandlerSignatureError("ingest", ("message", "extra"))

    assert error.handler_name == "ingest"
    assert error.parameters == ("message", "extra")
    assert "ingest declares (message, extra)" in str(error)
    assert "annotated Envelope" in str(error)


def test_handler_signature_error_shows_none_for_no_parameters() -> None:
    assert "(<none>)" in str(HandlerSignatureError("ingest", ()))


def test_invalid_dsn_error_carries_the_dsn() -> None:
    error = InvalidDsnError("just-a-host")

    assert error.dsn == "just-a-host"
    assert "has no scheme" in str(error)


def test_invalid_dsn_error_hides_credentials_in_its_message() -> None:
    """The DSN shape this error is actually raised for: credentials, and the
    missing ``://`` that is why it is raised at all."""
    error = InvalidDsnError("user:s3cret@host/vh")

    assert "s3cret" not in str(error)


def test_invalid_transport_option_error_carries_the_option_value_and_expected() -> None:
    error = InvalidTransportOptionError("prefetch_count", "abc", "an integer")

    assert error.option == "prefetch_count"
    assert error.value == "abc"
    assert error.expected == "an integer"
    assert "prefetch_count='abc' must be an integer" in str(error)


def test_message_decoding_failed_error_names_the_message_when_known() -> None:
    error = MessageDecodingFailedError("bad shape", "test.v1")

    assert error.reason == "bad shape"
    assert error.message_name == "test.v1"
    assert "cannot decode envelope for 'test.v1': bad shape" in str(error)


def test_message_decoding_failed_error_omits_the_name_when_unknown() -> None:
    error = MessageDecodingFailedError("bad shape")

    assert error.message_name is None
    assert "cannot decode envelope: bad shape" in str(error)


def test_message_encoding_failed_error_carries_the_reason_and_name() -> None:
    error = MessageEncodingFailedError("no codec", "test.v1")

    assert error.reason == "no codec"
    assert error.message_name == "test.v1"
    assert "cannot encode 'test.v1': no codec" in str(error)


def test_mixed_dsn_error_carries_the_conflicting_dsns() -> None:
    error = MixedDsnError(("amqp://a", "amqp://b"))

    assert error.dsns == ("amqp://a", "amqp://b")
    assert "got 2 DSNs" in str(error)


def test_mixed_dsn_error_redacts_the_dsns_it_carries() -> None:
    """The attribute reaches logs and error reports as easily as the message
    does, so the credentials are stripped before they are stored."""
    error = MixedDsnError(("amqp://user:s3cret@a/vh", "amqp://user:s3cret@b/vh"))

    assert error.dsns == ("amqp://a/vh", "amqp://b/vh")


def test_no_handler_for_message_error_carries_the_type_and_handled_types() -> None:
    error = NoHandlerForMessageError(Sample, ("some.other.Handled",))

    assert error.message_type is Sample
    assert error.handled_types == ("some.other.Handled",)
    assert "Sample" in str(error)
    assert "handled types: some.other.Handled" in str(error)


def test_no_handler_for_message_error_shows_none_for_no_handled_types() -> None:
    assert "handled types: <none>" in str(NoHandlerForMessageError(Sample, ()))


def test_no_sender_for_message_error_carries_the_type_and_routed_types() -> None:
    error = NoSenderForMessageError(Sample, ("IngestDocument",))

    assert error.message_type is Sample
    assert error.routed_types == ("IngestDocument",)
    assert "routed types: IngestDocument" in str(error)


def test_no_sender_for_message_error_shows_none_for_no_routed_types() -> None:
    assert "routed types: <none>" in str(NoSenderForMessageError(Sample, ()))


def test_not_consumable_error_carries_the_names_and_scheme() -> None:
    error = NotConsumableError(("q1", "q2"), "sync")

    assert error.names == ("q1", "q2")
    assert error.scheme == "sync"
    assert "cannot consume q1, q2" in str(error)


def test_unknown_message_name_error_carries_the_name() -> None:
    error = UnknownMessageNameError("bad.name")

    assert error.name == "bad.name"
    assert "no message type registered for name 'bad.name'" in str(error)


def test_unknown_transport_error_carries_the_names_and_known() -> None:
    error = UnknownTransportError(("nowhere",), ("other", "async"))

    assert error.names == ("nowhere",)
    assert error.known == ("other", "async")
    assert "unknown transport(s): nowhere" in str(error)
    assert "defined: async, other" in str(error)


def test_unknown_transport_error_shows_none_for_no_known_transports() -> None:
    assert "defined: <none>" in str(UnknownTransportError(("nowhere",), ()))


def test_unknown_middleware_error_carries_the_name_and_known() -> None:
    error = UnknownMiddlewareError("loging", ("logging",))

    assert error.name == "loging"
    assert error.known == ("logging",)
    assert "unknown middleware: loging" in str(error)
    assert "registered: logging" in str(error)


def test_unknown_middleware_error_shows_none_for_no_registered_middleware() -> None:
    assert "registered: <none>" in str(UnknownMiddlewareError("logging", ()))


def test_unknown_transport_option_error_carries_the_scheme_unknown_and_known() -> None:
    error = UnknownTransportOptionError("amqp", ("typo",), ("queue",))

    assert error.scheme == "amqp"
    assert error.unknown == ("typo",)
    assert error.known == ("queue",)
    assert "amqp:// does not accept typo" in str(error)
    assert "it accepts: queue" in str(error)


def test_unknown_transport_option_error_shows_none_for_no_known_options() -> None:
    assert "it accepts: <none>" in str(UnknownTransportOptionError("amqp", ("typo",), ()))


def test_unsupported_dsn_error_carries_the_transport_name_and_dsn() -> None:
    error = UnsupportedDsnError("jobs", "kafka://host")

    assert error.transport_name == "jobs"
    assert error.dsn == "kafka://host"
    assert "no transport factory for 'jobs'" in str(error)


def test_unsupported_dsn_error_hides_credentials_in_its_message() -> None:
    error = UnsupportedDsnError("jobs", "amqp://user:s3cret@host/vh")

    assert "s3cret" not in str(error)


#: The DSN shapes a well-formed authority never catches, each paired with the
#: fragments of its password; both DSN errors render the DSN into their
#: message, so none of them may survive in either.
_LEAKY_DSNS = (
    ("amqp://user:s3/cret@host/vh", ("s3", "cret")),
    ("amqp://user:s3?cret@host/vh", ("s3", "cret")),
    ("amqp://user:s3#cret@host/vh", ("s3", "cret")),
    ("user:s3/cret@host/vh", ("s3", "cret")),
    ("user:s3cret@host/vh?next=amqp://x", ("s3", "cret")),
    ("user:s3cret", ("s3", "cret")),
    ("rabbit:guest", ("guest",)),
)


@pytest.mark.parametrize(("dsn", "secrets"), _LEAKY_DSNS)
def test_invalid_dsn_error_hides_credentials_the_authority_path_misses(
    dsn: str,
    secrets: tuple[str, ...],
) -> None:
    message = str(InvalidDsnError(dsn))

    for secret in secrets:
        assert secret not in message


@pytest.mark.parametrize(("dsn", "secrets"), _LEAKY_DSNS)
def test_unsupported_dsn_error_hides_credentials_the_authority_path_misses(
    dsn: str,
    secrets: tuple[str, ...],
) -> None:
    message = str(UnsupportedDsnError("jobs", dsn))

    for secret in secrets:
        assert secret not in message


def test_unsupported_stamp_error_names_the_stamp_and_transport() -> None:
    error = UnsupportedStampError("DelayStamp", "sync")

    assert error.stamp_name == "DelayStamp"
    assert error.transport == "sync"
    assert "sync" in str(error)
    assert "DelayStamp" in str(error)


def test_incompatible_receivers_error_names_both_sides() -> None:
    error = IncompatibleReceiversError(("jobs",), ("scheduler_default",))

    assert (error.brokered, error.registered) == (("jobs",), ("scheduler_default",))
    assert "cannot consume jobs together with scheduler_default" in str(error)
