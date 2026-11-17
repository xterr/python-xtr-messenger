"""The package and its middleware import without the container, which is not a dependency.

The dev group installs the container, so nothing else in this suite can
express its absence: a fresh interpreter with it blanked out can.
"""

from __future__ import annotations

import subprocess
import sys

_SCRIPT = (
    "import sys; "
    "sys.modules['xtr_dependency_injection'] = None; "
    "sys.modules['xtr_service_contracts'] = None; "
    "import xtr_messenger, xtr_messenger.middleware"
)


def test_it_imports_without_the_container() -> None:
    completed = subprocess.run(
        [sys.executable, "-c", _SCRIPT], capture_output=True, text=True, check=False
    )

    assert completed.returncode == 0, completed.stderr
