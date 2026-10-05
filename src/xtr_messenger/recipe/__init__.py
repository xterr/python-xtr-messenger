"""This directory holds the package's recipe: its manifest and its templates.

The recipe is what *Use in an application* in the README lists, written as
data: the bundle to list, the configuration module to write, the environment
variable to declare, and the steps only a person can take. An application
applies it with ``xtr-recipes recipes:sync``, which finds this module through
the ``xtr_recipes`` entry point.

Nothing is importable from here. The manifest and the templates are read as
files, so a recipe is the same from a wheel as from a working tree.
"""

from __future__ import annotations

__all__: list[str] = []
