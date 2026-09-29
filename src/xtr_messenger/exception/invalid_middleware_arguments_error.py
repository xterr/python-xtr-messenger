"""Middleware was given arguments it does not take."""

from __future__ import annotations

from .message_bus_error import MessageBusError

__all__ = ["InvalidMiddlewareArgumentsError"]


class InvalidMiddlewareArgumentsError(MessageBusError):
    """The configuration gives middleware arguments it does not take.

    A configuration entry ``{name: {argument: value}}`` naming an argument
    the middleware's builder does not take, or written in another shape, is
    refused. With a container, an argument naming no constructor parameter
    fails the build like any definition argument.

    Attributes:
        name: The middleware the arguments were for.
        reason: What is wrong with them.
    """

    name: str
    reason: str

    def __init__(self, name: str, reason: str) -> None:
        """Record which middleware was misconfigured, and how."""
        self.name = name
        self.reason = reason
        super().__init__(f"invalid arguments for middleware {name}: {reason}")
