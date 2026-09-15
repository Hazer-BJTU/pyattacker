"""``python -m pyattacker`` — the same entry point as the console script.

Used by ``pyattacker run --shards N``, which spawns child processes this way so it works even
when the package is imported from a source checkout rather than an installed script.
"""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
