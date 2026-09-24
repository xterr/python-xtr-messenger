"""How the driver opens the connection, written back onto the URL."""

from __future__ import annotations

import pytest

from xtr_messenger import InvalidTransportOptionError
from xtr_messenger.bridge.amqp.connection_options import ConnectionOptions

_HOST = "amqp://guest:guest@localhost:5672/"


def test_the_defaults() -> None:
    options = ConnectionOptions()

    assert options.heartbeat is None
    assert options.connect_timeout is None
    assert options.connection_name is None
    assert options.frame_max is None
    assert options.channel_max is None
    assert options.verify is True


def test_from_settings_reads_every_field() -> None:
    options = ConnectionOptions.from_settings(
        {
            "heartbeat": "30",
            "connect_timeout": "5",
            "connection_name": "w1",
            "frame_max": "131072",
            "channel_max": "16",
            "cacert": "/ca.pem",
            "cert": "/c.pem",
            "key": "/k.pem",
            "verify": "false",
        },
    )

    assert options.heartbeat == 30.0
    assert options.connect_timeout == 5.0
    assert options.connection_name == "w1"
    assert options.frame_max == 131072
    assert options.channel_max == 16
    assert options.cacert == "/ca.pem"
    assert options.verify is False


def test_settings_override_the_given_defaults() -> None:
    built = ConnectionOptions.from_settings(
        {"heartbeat": "60"},
        ConnectionOptions(heartbeat=1.0, connection_name="kept"),
    )

    assert built.heartbeat == 60.0
    assert built.connection_name == "kept"


def test_connection_parameters_are_written_onto_the_url() -> None:
    """The driver reads these from the URL, so that is where they are put."""
    options = ConnectionOptions(
        heartbeat=30.0,
        connect_timeout=5.0,
        connection_name="w1",
        frame_max=131072,
        channel_max=16,
    )

    url = options.applied_to(_HOST)

    assert "heartbeat=30" in url
    assert "connection_timeout=5" in url
    assert "name=w1" in url
    assert "frame_max=131072" in url
    assert "channel_max=16" in url


def test_tls_material_is_written_onto_the_url() -> None:
    options = ConnectionOptions(cacert="/ca.pem", cert="/c.pem", key="/k.pem", verify=False)

    url = options.applied_to(_HOST)

    assert "cafile=%2Fca.pem" in url
    assert "certfile=%2Fc.pem" in url
    assert "keyfile=%2Fk.pem" in url
    assert "no_verify_ssl=1" in url


def test_a_verified_connection_writes_no_no_verify_flag() -> None:
    url = ConnectionOptions(heartbeat=30.0).applied_to(_HOST)

    assert "no_verify_ssl" not in url


def test_an_existing_query_string_is_kept() -> None:
    url = ConnectionOptions(heartbeat=30.0).applied_to(f"{_HOST}?vhost=prod")

    assert "vhost=prod" in url
    assert "heartbeat=30" in url


def test_a_url_is_unchanged_when_nothing_is_set() -> None:
    assert ConnectionOptions().applied_to(_HOST) == _HOST


def test_a_non_numeric_heartbeat_is_refused() -> None:
    with pytest.raises(InvalidTransportOptionError, match="heartbeat"):
        _ = ConnectionOptions.from_settings({"heartbeat": "often"})


def test_a_non_numeric_frame_max_is_refused() -> None:
    with pytest.raises(InvalidTransportOptionError, match="frame_max"):
        _ = ConnectionOptions.from_settings({"frame_max": "big"})
