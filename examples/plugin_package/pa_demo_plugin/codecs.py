"""A codec published under the ``pyattacker.codecs`` entry-point group."""

from __future__ import annotations

from typing import Any

from pyattacker import Codec


class CsvRowCodec(Codec):
    """Encode a ``list[str]`` as one CSV line instead of a JSON array.

    Registered automatically on the first ``Runner`` (or explicitly via
    ``PLUGINS.install_codecs(registry)``), and then selected by type.
    """

    name = "csv_row"

    def can_encode(self, obj: Any) -> bool:
        return isinstance(obj, list) and all(isinstance(item, str) for item in obj)

    def dumps(self, obj: Any) -> bytes:
        import csv
        import io

        buffer = io.StringIO()
        csv.writer(buffer, lineterminator="").writerow(obj)
        return buffer.getvalue().encode("utf-8")

    def loads(self, data: bytes) -> Any:
        import csv
        import io

        return next(csv.reader(io.StringIO(data.decode("utf-8"))))
