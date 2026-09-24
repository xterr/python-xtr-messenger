from __future__ import annotations

from message_bus import MessageBusConfig, TransportConfig
from tests.support.messages import IngestDocument


def test_a_config_builds_nothing_by_itself() -> None:
    config = MessageBusConfig(transports={"sync": TransportConfig("sync://")})

    built = {"bus", "senders", "locator", "consumer"} & set(dir(config))

    assert built == set()


def test_the_routing_map_is_optional() -> None:
    config = MessageBusConfig(transports={"sync": TransportConfig("sync://")})

    assert config.routing == {}


def test_a_config_holds_the_transports_it_was_given() -> None:
    config = MessageBusConfig(transports={"sync": TransportConfig("sync://")})

    assert config.transports["sync"].dsn == "sync://"


def test_a_config_holds_the_routing_it_was_given() -> None:
    config = MessageBusConfig(
        transports={"sync": TransportConfig("sync://")},
        routing={IngestDocument: "sync"},
    )

    assert config.routing[IngestDocument] == "sync"


def test_by_connection_groups_transports_that_differ_only_in_the_query_string() -> None:
    """Transports differing only in their query string share one connection."""
    config = MessageBusConfig(
        transports={
            "high": TransportConfig("amqp://rabbit:5672/?queue=jobs_high"),
            "low": TransportConfig("amqp://rabbit:5672/?queue=jobs_low"),
        },
    )

    grouped = config.by_connection()

    assert len(grouped) == 1
    [group] = grouped.values()
    assert set(group) == {"high", "low"}


def test_by_connection_separates_transports_on_different_servers() -> None:
    config = MessageBusConfig(
        transports={
            "here": TransportConfig("amqp://rabbit-a/?queue=jobs"),
            "there": TransportConfig("amqp://rabbit-b/?queue=jobs"),
        },
    )

    assert len(config.by_connection()) == 2


def test_by_connection_regroups_the_transport_configs_untouched() -> None:
    high = TransportConfig("amqp://rabbit/?queue=jobs_high")
    config = MessageBusConfig(transports={"high": high})

    [group] = config.by_connection().values()

    assert group["high"] is high
