"""Dispatched when a worker handled a message, before acknowledging it."""

from __future__ import annotations

from typing import final

from .abstract_worker_message_event import AbstractWorkerMessageEvent

__all__ = ["WorkerMessageHandledEvent"]


@final
class WorkerMessageHandledEvent(AbstractWorkerMessageEvent):
    """A message was handled; the envelope carries what the handlers recorded.

    Dispatched before the transport is told, so a stamp a listener adds is on
    the envelope that is acknowledged.
    """
