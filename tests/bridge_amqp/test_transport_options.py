from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from taskiq import AsyncBroker, SmartRetryMiddleware

from message_bus import (
    InvalidDsnError,
    MessageBusConfig,
    TransportConfig,
    UnknownTransportOptionError,
    WorkerFactory,
)
from message_bus.bridge.amqp.amqp_options import AmqpOptions
from message_bus.bridge.amqp.dead_letter_middleware import DeadLetterMiddleware
from message_bus.bridge.amqp.reliability import Reliability
from message_bus.bridge.taskiq.taskiq_worker import TaskiqWorker

if TYPE_CHECKING:
    from collections.abc import Mapping

    from taskiq_aio_pika.exchange import Exchange
    from taskiq_aio_pika.queue import Queue

HOST = "amqp://guest:guest@localhost:5672/"


def broker_for(dsn: str, options: Mapping[str, str] | None = None) -> AsyncBroker:
    spec = TransportConfig(dsn) if options is None else TransportConfig(dsn, options=options)
    worker = WorkerFactory(MessageBusConfig(transports={"q": spec})).worker(["q"])
    assert isinstance(worker, TaskiqWorker)
    return worker.broker


def url_of(broker: AsyncBroker) -> str:
    return str(cast("object", vars(broker)["url"]))


def queue_of(broker: AsyncBroker) -> Queue:
    return cast("list[Queue]", vars(broker)["_task_queues"])[0]


def exchange_of(broker: AsyncBroker) -> Exchange:
    return cast("Exchange", vars(broker)["_exchange"])


def retry_of(broker: AsyncBroker) -> SmartRetryMiddleware:
    found = [m for m in broker.middlewares if isinstance(m, SmartRetryMiddleware)]
    assert found
    return found[0]


def test_reliability_reaches_the_broker_from_the_dsn() -> None:
    """The settings were parsed but never read, so a configured retry policy
    silently ran on defaults."""
    broker = broker_for(f"{HOST}?queue=jobs&max_attempts=10&base_delay_seconds=2.5")

    retry = retry_of(broker)

    assert retry.default_retry_count == 10
    assert retry.default_delay == 2.5


def test_the_dead_letter_queue_is_configurable() -> None:
    broker = broker_for(f"{HOST}?queue=jobs&dead_letter_queue=my.dlq")

    dead_letter = [m for m in broker.middlewares if isinstance(m, DeadLetterMiddleware)]

    assert vars(dead_letter[0])["queue_name"] == "my.dlq"


def test_prefetch_and_exchange_reach_the_broker() -> None:
    broker = broker_for(f"{HOST}?queue=jobs&prefetch_count=50&exchange=jobs.ex")

    assert vars(broker)["_qos"] == 50
    assert exchange_of(broker).name == "jobs.ex"


def test_connection_settings_reach_the_driver_as_uri_parameters() -> None:
    """The driver reads these from the URL, so that is where they are put."""
    broker = broker_for(f"{HOST}?queue=jobs&heartbeat=30&connect_timeout=5&connection_name=w1")

    url = url_of(broker)

    assert "heartbeat=30" in url
    assert "connection_timeout=5" in url
    assert "name=w1" in url


def test_tls_material_reaches_the_driver() -> None:
    broker = broker_for(f"{HOST}?queue=jobs&cacert=/ca.pem&cert=/c.pem&key=/k.pem&verify=false")

    url = url_of(broker)

    assert "cafile=%2Fca.pem" in url
    assert "certfile=%2Fc.pem" in url
    assert "keyfile=%2Fk.pem" in url
    assert "no_verify_ssl=1" in url


def test_the_queue_and_exchange_topology_is_configurable() -> None:
    broker = broker_for(
        f"{HOST}?queue=jobs&queue_type=classic&queue_max_priority=5&exchange=jobs.ex&exchange_type=direct"
    )

    queue = queue_of(broker)
    exchange = exchange_of(broker)

    assert queue.type.value == "classic"
    assert queue.max_priority == 5
    assert exchange.type.value == "direct"


def test_auto_setup_off_stops_the_transport_declaring_topology() -> None:
    """For a deployment that creates its own topology and grants the
    application no permission to."""
    broker = broker_for(f"{HOST}?queue=jobs&exchange=jobs.ex&auto_setup=false")

    assert queue_of(broker).declare is False
    assert exchange_of(broker).declare is False


def test_an_unusable_enum_value_names_what_is_allowed() -> None:
    with pytest.raises(InvalidDsnError, match="quorum"):
        _ = broker_for(f"{HOST}?queue=jobs&queue_type=nonsense")


def test_options_override_the_dsn() -> None:
    """A DSN lives in an environment variable; options let code override it."""
    broker = broker_for(f"{HOST}?queue=jobs&max_attempts=3", options={"max_attempts": "9"})

    assert retry_of(broker).default_retry_count == 9


def test_a_misspelled_option_is_refused_rather_than_ignored() -> None:
    """The whole bug: an unread setting leaves the transport on defaults
    nobody chose."""
    with pytest.raises(UnknownTransportOptionError, match="max_attempt"):
        _ = broker_for(f"{HOST}?queue=jobs&max_attempt=10")


def test_an_option_meant_for_another_scheme_is_refused() -> None:
    with pytest.raises(UnknownTransportOptionError, match="serialize"):
        _ = broker_for(f"{HOST}?queue=jobs&serialize=true")


def test_a_non_numeric_value_fails_at_startup() -> None:
    with pytest.raises(InvalidDsnError, match="max_attempts"):
        _ = broker_for(f"{HOST}?queue=jobs&max_attempts=lots")


def test_options_given_in_code_are_the_defaults_the_dsn_overrides() -> None:
    settings = {"max_attempts": "7"}
    built = AmqpOptions.from_settings(
        settings,
        AmqpOptions(reliability=Reliability(base_delay_seconds=9.0), prefetch_count=99),
    )

    assert built.reliability.max_attempts == 7
    assert built.reliability.base_delay_seconds == 9.0
    assert built.prefetch_count == 99
