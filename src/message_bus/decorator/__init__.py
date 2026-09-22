"""The declarations an application writes on its own classes.

Symfony spells these as PHP attributes — ``#[AsMessage]``,
``#[AsMessageHandler]``. Python's equivalent is a decorator, and the word
matters here: *attribute* already means something else in Python, so the
package is named for what these actually are.
"""

from .as_message import as_message
from .as_message_handler import as_message_handler

__all__ = ["as_message", "as_message_handler"]
