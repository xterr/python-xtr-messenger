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
* :func:`~message_bus.bridge.taskiq.binding.bind_handlers` registers handlers
  declared with :func:`~message_bus.decorator.as_message_handler` as
  tasks on a broker, which is why handler modules never import one.

The RabbitMQ wiring built on top of this — connection, retry ladder,
dead-lettering — lives in :mod:`message_bus.bridge.amqp` and needs the
``amqp`` extra.
"""

from .binding import bind_handlers
from .broker import MissingTaskRouteError, assert_routes_registered, ensure_started
from .taskiq_sender import TaskiqSender
from .taskiq_worker import TaskiqWorker

__all__ = [
    "MissingTaskRouteError",
    "TaskiqSender",
    "TaskiqWorker",
    "assert_routes_registered",
    "bind_handlers",
    "ensure_started",
]
