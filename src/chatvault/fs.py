"""Small filesystem helpers shared across the codebase."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Literal

LinkMode = Literal["link", "copy", "noop"]


def link_or_copy(src: Path, dst: Path) -> LinkMode:
    """Materialise ``dst`` from ``src``. Returns the mode used.

    Hard-link is attempted only when ``os.link`` exists (Android's bionic libc
    on some Termux builds omits it) and the kernel allows it across the
    source/destination filesystem (FUSE-mounted sdcard rejects hard links —
    we fall back to copy2 in that case).
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return "noop"
    if hasattr(os, "link"):
        try:
            os.link(src, dst)
            return "link"
        except (OSError, AttributeError):
            pass
    shutil.copy2(src, dst)
    return "copy"
