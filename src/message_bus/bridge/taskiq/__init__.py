"""The taskiq bridge — broker-agnostic.

Installed with the ``taskiq`` extra. Nothing here names a broker: these
pieces work with any taskiq broker, so wiring a second one costs a factory
and no changes to this package.

* :class:`~message_bus.bridge.taskiq.taskiq_sender.TaskiqSender` publishes an
  envelope as a taskiq task, addressed by the message's own name.
* :class:`~message_bus.bridge.taskiq.taskiq_worker.TaskiqWorker` consumes by
  running taskiq's own worker behind
  :class:`~message_bus.worker_interface.WorkerInterface`, so callers stay free of
  taskiq.
* :func:`~message_bus.bridge.taskiq.binding.bind_bus` registers every
  declared message as a task on a broker, dispatching into a bus — which is
  why handler modules never import one.

The RabbitMQ wiring built on top of this — connection, retry ladder,
dead-lettering — lives in :mod:`message_bus.bridge.amqp` and needs the
``amqp`` extra.
"""

from .binding import bind_bus
from .broker import ensure_started
from .taskiq_sender import TaskiqSender
from .taskiq_worker import TaskiqWorker

__all__ = [
    "TaskiqSender",
    "TaskiqWorker",
    "bind_bus",
    "ensure_started",
]
