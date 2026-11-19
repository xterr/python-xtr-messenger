"""``debug:messenger``: what can be dispatched, who handles it, and where it goes."""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import final

from xtr_console import ConsoleStyle, ExitCode, as_command, escape

from xtr_messenger._text import class_name
from xtr_messenger.exception import UnknownMessageNameError
from xtr_messenger.handler.handlers_locator import HandlersLocator
from xtr_messenger.message_bus_config import MessageBusConfig
from xtr_messenger.message_registry import declared_names, transports_of, type_for_name
from xtr_messenger.transport.sender.senders_locator import WILDCARD, SendersLocator

__all__ = ["DebugCommand"]

_DEFAULT_FIRST = "DispatchAfterCurrentBusMiddleware"
_DEFAULT_LAST = ("SendMessageMiddleware", "HandleMessageMiddleware")


@as_command("debug:messenger")
@final
class DebugCommand:
    """Lists the messages that can be dispatched, their handlers, routes and failure transports.

    A container builds it with the configuration and handlers the messenger
    bundle holds.
    """

    __slots__ = ("_config", "_handlers")

    def __init__(self, config: MessageBusConfig, handlers: HandlersLocator) -> None:
        """Describe ``config``'s routing and the handlers ``handlers`` holds."""
        self._config = config
        self._handlers = handlers

    async def __call__(
        self, io: ConsoleStyle, *, message: str | None = None, middleware: bool = False
    ) -> int:
        """List messages you can dispatch using the message bus.

        Routing comes from the configuration, then from what a message
        declares with ``as_message``; a ``TransportNamesStamp`` overrides it
        at dispatch time.

        Args:
            io: Where the command writes.
            message: The message to inspect — its ``module.QualName``, or the
                name it was declared under.
            middleware: Also list the middleware the configuration names, in
                the order they are called — an integration may add its own
                around them, as the bundle opens a unit of work first.
        """
        io.title("Messenger")
        known = self._message_types()
        if message is None:
            types = sorted(known, key=class_name_of)
        else:
            found = _resolve(message, known)
            if found is None:
                io.error(f'The message "{escape(message)}" is not known.')
                return ExitCode.FAILURE
            types = [found]
        if middleware:
            io.section("Middleware")
            io.text("The middleware the configuration names, in the order they are called:")
            io.listing(escape(name) for name in self._middleware_names())
        io.section("Messages")
        if not types:
            io.warning("No handled or routed message found.")
        else:
            io.table(["Message", "Handled by", "Routed to"], [self._message_row(t) for t in types])
        if self._config.transports:
            io.section("Transports")
            io.info("TransportNamesStamp can override this routing at dispatch time.")
            io.table(
                ["Transport", "Routing rules", "Failed messages are routed to"],
                [self._transport_row(name, types) for name in sorted(self._config.transports)],
            )
        return ExitCode.SUCCESS

    def _message_types(self) -> set[type]:
        """Return every message handled, routed by the configuration, or declared with a route."""
        known = set(self._handlers.message_types())
        known.update(key for key in self._config.routing if isinstance(key, type))
        for name in declared_names():
            declared = type_for_name(name)
            if transports_of(declared):
                known.add(declared)
        return known

    def _message_row(self, message_type: type) -> list[str]:
        description = _description(message_type)
        shown = escape(class_name_of(message_type))
        handlers = [
            escape(class_name_of(descriptor.handler))
            for descriptor in self._handlers.handlers_for(message_type)
        ]
        routes = dict.fromkeys(
            SendersLocator.transport_names_for(message_type, self._config.routing)
        )
        return [
            f"{shown}\n[dim]{escape(description)}[/]" if description else shown,
            "\n".join(handlers) or "[yellow]not handled[/]",
            "\n".join(escape(name) for name in routes) or "[yellow]not routed[/]",
        ]

    def _transport_row(self, name: str, types: list[type]) -> list[str]:
        rules: list[str] = []
        for key, routed in self._config.routing.items():
            if name not in _names(routed):
                continue
            if key == WILDCARD:
                rules.append("* (fallback for messages with no other route)")
            else:
                rules.append(escape(key if isinstance(key, str) else class_name_of(key)))
        for message_type in types:
            if self._routed_by_configuration(message_type):
                continue
            if name in transports_of(message_type):
                rules.append(f"{escape(class_name_of(message_type))} (from as_message)")
        failure = self._config.failure_transport_of(name)
        return [
            escape(name),
            "\n".join(rules) or "[yellow]no routing rules[/]",
            escape(failure) if failure is not None else "-",
        ]

    def _routed_by_configuration(self, message_type: type) -> bool:
        routing = self._config.routing
        return WILDCARD in routing or any(base in routing for base in message_type.__mro__)

    def _middleware_names(self) -> list[str]:
        named: list[str] = []
        for entry in self._config.middleware:
            if isinstance(entry, str):
                named.append(entry)
            elif isinstance(entry, Mapping):
                named.extend(str(key) for key in entry)
            else:
                named.append(class_name(entry))
        if not self._config.default_middleware:
            return named
        return [_DEFAULT_FIRST, *named, *_DEFAULT_LAST]


def class_name_of(kind: object) -> str:
    """Return ``module.QualName`` of a class or function, or of an object's class."""
    qualname = getattr(kind, "__qualname__", None)
    module = getattr(kind, "__module__", None)
    if isinstance(qualname, str) and isinstance(module, str):
        return f"{module}.{qualname}"
    return class_name(kind)


def _resolve(message: str, known: set[type]) -> type | None:
    """Return the message class ``message`` names — by ``module.QualName``, or its wire name."""
    for candidate in known:
        if class_name_of(candidate) == message:
            return candidate
    try:
        return type_for_name(message)
    except UnknownMessageNameError:
        return None


def _description(kind: type) -> str:
    """Return the first line of what ``kind`` documents itself as, if it does.

    Not a dataclass's own made-up signature, which says nothing a reader
    does not see in the class name already.
    """
    documented = inspect.getdoc(kind) or ""
    first = documented.strip().split("\n\n", 1)[0].replace("\n", " ")
    return "" if first.startswith(f"{kind.__name__}(") else first


def _names(routed: str | Sequence[str]) -> tuple[str, ...]:
    return (routed,) if isinstance(routed, str) else tuple(routed)
