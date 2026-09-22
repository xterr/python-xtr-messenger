"""The declarations an application writes on its own classes.

Named for what they are rather than what they do: *attribute* already means
something else in Python, and these are decorators.
"""

from .as_message import as_message
from .as_message_handler import as_message_handler

__all__ = ["as_message", "as_message_handler"]
