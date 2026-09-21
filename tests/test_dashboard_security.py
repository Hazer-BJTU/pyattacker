"""Untrusted stored fields must never be interpreted as dashboard HTML."""

from __future__ import annotations

import re
from html.parser import HTMLParser

from pyattacker import MemoryStore
from pyattacker.server import StatsServer
from pyattacker.store.base import EventRecord


def test_dashboard_scripts_do_not_use_html_parsing_sinks():
    # The page needs only text and explicitly created elements. Guard the whole
    # script, including cards, so a new table cannot silently reintroduce #58.
    class Scripts(HTMLParser):
        def __init__(self):
            super().__init__()
            self.inside = False
            self.source = []

        def handle_starttag(self, tag, attrs):
            if tag == "script":
                self.inside = True

        def handle_endtag(self, tag):
            if tag == "script":
                self.inside = False

        def handle_data(self, data):
            if self.inside:
                self.source.append(data)

    store = MemoryStore()
    try:
        status, body = StatsServer(store).payload("/", {})
        assert status == 200
        parser = Scripts()
        parser.feed(body["_html"])
        scripts = "\n".join(parser.source)
        assert scripts
        assert not re.search(r"\b(?:innerHTML|outerHTML|insertAdjacentHTML|createContextualFragment)\b", scripts)
        assert not re.search(r"\bdocument\s*\.\s*write(?:ln)?\s*\(", scripts)
    finally:
        store.close()


def test_event_api_preserves_html_like_text_for_safe_display():
    # Escape at the rendering boundary: JSON consumers still need the original
    # event data, including quotes, ampersands and strings that resemble markup.
    payload = '<img src=x onerror=alert(1)> & "quoted" </script>'
    store = MemoryStore()
    try:
        store.emit_event(EventRecord(
            ts=1, kind="failed." + payload, pipeline_id="<b>id</b>", data={"message": payload}
        ))
        status, body = StatsServer(store).payload("/events", {"run_id": ["all"]})
        assert status == 200
        event = body["rows"][0]
        assert event["kind"] == "failed." + payload
        assert event["pipeline_id"] == "<b>id</b>"
        assert event["data"] == {"message": payload}
    finally:
        store.close()
