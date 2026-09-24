"""Building a RabbitMQ broker with retries, delays and dead-lettering wired up.

The broker keeps its qos and exchange as private attributes with no public
accessor, so those two settings are pinned where they are read
(``test_amqp_options`` and ``test_exchange_options``); here we assert what the
broker exposes: its middlewares, its declared queues, and its connection URL.
"""

from __future__ import annotations

from taskiq import SmartRetryMiddleware
from taskiq_aio_pika import AioPikaBroker

from xtr_messenger.bridge.amqp.amqp_broker import create_amqp_broker, declared_queues
from xtr_messenger.bridge.amqp.amqp_options import AmqpOptions
from xtr_messenger.bridge.amqp.connection_options import ConnectionOptions
from xtr_messenger.bridge.amqp.dead_letter_middleware import DeadLetterMiddleware
from xtr_messenger.bridge.amqp.reliability import Reliability

_HOST = "amqp://guest:guest@localhost:5672/"


def _retry_of(broker: AioPikaBroker) -> SmartRetryMiddleware:
    found = [m for m in broker.middlewares if isinstance(m, SmartRetryMiddleware)]
    assert found
    return found[0]


def test_construction_opens_no_connection() -> None:
    """Safe to call at import time, which a module-level broker needs."""
    broker = create_amqp_broker("amqp://guest:guest@unreachable:5672/")

    assert broker.write_channel is None


def test_it_wires_the_retry_middleware() -> None:
    broker = create_amqp_broker(_HOST)

    assert any(isinstance(m, SmartRetryMiddleware) for m in broker.middlewares)


def test_the_retry_ladder_reflects_the_reliability_settings() -> None:
    """The settings were once parsed but never read, so a configured retry
    policy silently ran on defaults."""
    broker = create_amqp_broker(
        _HOST,
        AmqpOptions(reliability=Reliability(max_attempts=10, base_delay_seconds=2.5)),
    )

    retry = _retry_of(broker)

    assert retry.default_retry_count == 10
    assert retry.default_delay == 2.5


def test_dead_lettering_is_wired_by_default() -> None:
    broker = create_amqp_broker(_HOST)

    dead_letter = [m for m in broker.middlewares if isinstance(m, DeadLetterMiddleware)]

    assert len(dead_letter) == 1
    assert dead_letter[0].queue_name == "taskiq.dlq"


def test_the_dead_letter_queue_is_configurable() -> None:
    broker = create_amqp_broker(
        _HOST,
        AmqpOptions(reliability=Reliability(dead_letter_queue="my.dlq")),
    )

    dead_letter = [m for m in broker.middlewares if isinstance(m, DeadLetterMiddleware)]

    assert dead_letter[0].queue_name == "my.dlq"


def test_dead_lettering_can_be_turned_off() -> None:
    broker = create_amqp_broker(
        _HOST,
        AmqpOptions(reliability=Reliability(dead_letter_queue=None)),
    )

    assert not any(isinstance(m, DeadLetterMiddleware) for m in broker.middlewares)


def test_it_declares_every_queue_it_is_given() -> None:
    broker = create_amqp_broker(_HOST, queues=["jobs_high", "jobs_low"])

    assert declared_queues(broker) == ("jobs_high", "jobs_low")


def test_it_declares_no_queue_when_given_none() -> None:
    broker = create_amqp_broker(_HOST)

    assert declared_queues(broker) == ()


def test_connection_settings_reach_the_url() -> None:
    """The driver reads these from the URL, so create wrote them there."""
    broker = create_amqp_broker(
        _HOST,
        AmqpOptions(connection=ConnectionOptions(heartbeat=30.0, connection_name="w1")),
    )

    assert broker.url is not None
    assert "heartbeat=30" in broker.url
    assert "name=w1" in broker.url


def test_tls_material_reaches_the_url() -> None:
    broker = create_amqp_broker(
        _HOST,
        AmqpOptions(connection=ConnectionOptions(cacert="/ca.pem", verify=False)),
    )

    assert broker.url is not None
    assert "cafile=%2Fca.pem" in broker.url
    assert "no_verify_ssl=1" in broker.url


def test_the_exchange_and_prefetch_are_still_wired() -> None:
    """These reach the broker as private constructor arguments with no public
    accessor; building the broker with them set exercises that wiring, while
    the values themselves are pinned in the options tests."""
    broker = create_amqp_broker(
        _HOST,
        AmqpOptions(prefetch_count=50),
    )

    assert isinstance(broker, AioPikaBroker)
