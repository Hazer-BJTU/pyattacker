"""A read-only HTTP view over a store — the zero-dependency monitoring endpoint.

Why the standard library: the whole point of this project is "few dependencies, easy to read".
``http.server`` plus a read-only SQLite connection per request is enough to serve a live
dashboard, and it keeps the monitoring story as auditable as the kernel.

Design notes:

* **Read-only by construction.** Every request opens the store read-only (``mode=ro``) and closes
  it again, so the server can run beside a live run without touching its writer.
* **No authentication, binds to loopback by default.** It exposes your run's payloads; treat it
  as a debug view, not as a public API. Put it behind your own proxy if you need one.
* **The JSON endpoints are the real interface** (``/stats``, ``/metrics``, ``/events``, ``/pipelines``,
  ``/resources``); the HTML page at ``/`` is a convenience built on top of them.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import ConfigError
from .monitor import read_snapshot, resolve_run_id
from .reported_metrics import read_reported_metrics
from .store.base import Store, open_store

__all__ = ["StatsServer", "serve"]

_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>pyattacker</title>
<style>
 body{font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;margin:0;padding:16px;background:#111;color:#ddd}
 h1{font-size:15px;margin:0 0 12px} .row{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px}
 .card{background:#1b1b1b;border:1px solid #333;border-radius:6px;padding:10px 12px;min-width:150px}
 .k{color:#888;font-size:11px;text-transform:uppercase} .v{font-size:18px}
 table{border-collapse:collapse;width:100%} td,th{text-align:left;padding:3px 8px;border-bottom:1px solid #262626}
 th{color:#888;font-weight:500;font-size:11px;text-transform:uppercase}
 .f{color:#f66} .s{color:#6c6} .w{color:#fc6}
</style></head><body>
<h1>pyattacker <span id="run" class="k"></span></h1>
<div class="row" id="cards"></div>
<h2>Suite experiments</h2><div id="experiments"></div>
<h2>Experiment metrics</h2><div class="row" id="metrics"></div>
<h2>Pipeline reports (first 50)</h2>
<table><thead><tr><th>pipeline</th><th>reported status</th></tr></thead><tbody id="pipeline-metrics"></tbody></table>
<h2>Events</h2>
<table><thead><tr><th>kind</th><th>pipeline</th><th>data</th></tr></thead><tbody id="events"></tbody></table>
<script>
function showMetrics(rows){
  const root = document.getElementById('metrics');
  root.replaceChildren();
  for(const row of rows){
    const card = document.createElement('div'); card.className = 'card';
    const label = document.createElement('div'); label.className = 'k';
    label.textContent = row.label || row.name;
    const value = document.createElement('div'); value.className = 'v';
    value.textContent = row.display === 'percent' ? `${(row.value * 100).toFixed(1)}%` : String(row.value);
    card.append(label, value); root.append(card);
  }
}
async function tick(){
  const requested = new URLSearchParams(location.search).get('run_id');
  const filters = new URLSearchParams(location.search);
  const statsPath = 'stats?' + filters.toString();
  const s = await (await fetch(statsPath)).json();
  let scope = 'run_id=' + encodeURIComponent(requested === 'all' ? 'all' : (s.run_id || 'all'));
  if(filters.has('experiment')) scope += '&experiment=' + encodeURIComponent(filters.get('experiment'));
  const members = document.getElementById('experiments'); members.replaceChildren();
  if(s.suite_id){
    const all = document.createElement('a');
    const overview = new URLSearchParams(location.search); overview.delete('experiment');
    all.href = '?' + overview.toString(); all.textContent = 'All experiments'; members.append(all);
  }
  for(const exp of (s.experiments || [])){
    const line = document.createElement('p');
    const link = document.createElement('a');
    const selected = new URLSearchParams(location.search); selected.set('experiment', exp.experiment_id);
    link.href = '?' + selected.toString(); link.textContent = exp.label || exp.experiment_id;
    line.append(link, document.createTextNode(' ' + exp.state + ' ' + JSON.stringify(exp.pipelines.by_state)));
    for(const metric of exp.reported_metrics || []){
      line.append(document.createTextNode(' ' + metric.name + '=' + String(metric.value)));
    }
    members.append(line);
  }
  document.getElementById('run').textContent = s.run_id || s.suite_id || 'all runs';
  const states = (s.pipelines||{}).by_state||{};
  const cards = [['total',(s.pipelines||{}).total||0],
    ...Object.entries(states).map(([k,v])=>[k,v]),
    ['attempts', s.attempts_total||0], ['events', s.events_total||0]];
  document.getElementById('cards').innerHTML = cards.map(([k,v])=>
    `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`).join('');
  const metrics = await (await fetch('metrics?' + scope)).json();
  showMetrics(metrics.rows);
  const pipelines = await (await fetch('pipelines?limit=50&' + scope)).json();
  const reports = document.getElementById('pipeline-metrics');
  reports.replaceChildren();
  for(const row of pipelines.rows){
    if(!row.reported_metrics.length) continue;
    const tr = document.createElement('tr');
    const id = document.createElement('td'); id.textContent = row.pipeline_id.slice(0,12);
    const details = document.createElement('td');
    details.textContent = row.reported_metrics.map(item =>
      `${item.label || item.name}: ${item.display === 'percent' ? (item.value * 100).toFixed(1) + '%' : item.value}`
    ).join(' · ');
    tr.append(id, details); reports.append(tr);
  }
  const ev = await (await fetch('events?limit=40&' + scope)).json();
  document.getElementById('events').innerHTML = ev.rows.map(r=>{
    const cls = r.kind.includes('failed')?'f':(r.kind.includes('succeeded')?'s':'');
    return `<tr><td class="${cls}">${r.kind}</td><td>${(r.pipeline_id||'').slice(0,12)}</td>`+
           `<td>${JSON.stringify(r.data).slice(0,160)}</td></tr>`;}).join('');
}
tick(); setInterval(tick, 1500);
</script></body></html>
"""


class StatsServer:
    """Serve a store over HTTP from a background thread.

    Usage::

        with StatsServer("runs/qa.db", port=8787) as server:
            print(server.url)      # http://127.0.0.1:8787
            ...
    """

    def __init__(
        self,
        store: Store | str,
        *,
        host: str = "127.0.0.1",
        port: int = 8787,
        run_id: str | None = None,
        errors: int = 10,
        experiment: str | None = None,
    ) -> None:
        self.store_spec = store
        self.host = host
        self.port = port
        self.run_id = run_id
        self.errors = errors
        self.experiment = experiment
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.requests = 0

    # ------------------------------------------------------------- lifecycle
    def start(self) -> "StatsServer":
        if self._httpd is not None:
            return self
        handler = _make_handler(self)
        try:
            self._httpd = ThreadingHTTPServer((self.host, self.port), handler)
        except OSError as exc:
            raise ConfigError(f"cannot bind {self.host}:{self.port}: {exc}") from exc
        self._httpd.daemon_threads = True
        self.port = self._httpd.server_address[1]  # resolves port 0 to the real one
        self._thread = threading.Thread(
            target=self._httpd.serve_forever, name="pyattacker-http", daemon=True
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._httpd is None:
            return
        self._httpd.shutdown()
        self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._httpd = None
        self._thread = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def wait(self) -> None:
        """Block until the server stops (used by the CLI, which has nothing else to do)."""
        if self._thread is not None:
            self._thread.join()

    def __enter__(self) -> "StatsServer":
        return self.start()

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()

    # ------------------------------------------------------------------ data
    def _read(self, fn: Any, experiment: str | None = None) -> Any:
        """Run one read against the store.

        A file store gets a fresh *read-only* connection per request: never the writer's
        connection, never a cross-thread handle, and it costs microseconds.
        """
        store = self.store_spec
        if isinstance(store, Store):
            return fn(store)
        if isinstance(store, str) and store not in (":memory:", "memory"):
            from .store.sqlite import SqliteStore

            if Path(store).is_dir():
                from .store.suite import SuiteStore
                handle = SuiteStore(store, read_only=True, experiment=experiment or self.experiment)
            else:
                handle = SqliteStore(store, read_only=True)
        else:
            handle = open_store(store)
        try:
            return fn(handle)
        finally:
            handle.close()

    def payload(self, path: str, query: dict[str, list[str]]) -> tuple[int, dict[str, Any]]:
        """Route one request to a JSON-able object. Returns ``(status, body)``."""
        self.requests += 1
        limit = _int(query.get("limit"), 50)
        run_id = (query.get("run_id") or [self.run_id])[0]

        experiment = (query.get("experiment") or [self.experiment])[0]
        def read(fn: Any) -> Any:
            return self._read(fn, experiment=experiment)

        if path in ("/healthz", "/health"):
            return 200, {"ok": True}

        if path in ("/", "/index.html"):
            return 200, {"_html": _PAGE}

        if path == "/stats":
            snapshot = read(lambda store: read_snapshot(store, run_id, errors=self.errors))
            snapshot.pop("buffered", None)
            return 200, snapshot

        if path == "/experiments":
            return 200, {"rows": read(lambda store: store.experiments(run_id) if getattr(store, "suite_store", False) else [])}

        if path == "/metrics":
            pipeline_id = (query.get("pipeline_id") or [None])[0]

            def _metrics(store: Any) -> Any:
                selected = resolve_run_id(store, run_id)
                rows = read_reported_metrics(store, run_id=selected, pipeline_id=pipeline_id) if selected or getattr(store, "suite_store", False) else []
                return selected, [
                    {"run_id": row.run_id, "pipeline_id": row.pipeline_id, "experiment_id": row.experiment_id,
                     "name": row.name, "value": row.value, "label": row.label,
                     "display": row.display, "updated_at": row.updated_at}
                    for row in rows
                ]

            selected, rows = read(_metrics)
            return 200, {"run_id": selected, "rows": rows}

        if path == "/events":
            rows = read(
                lambda store: [
                    {
                        "event_id": event.event_id,
                        "ts": event.ts,
                        "kind": event.kind,
                        "scope": event.scope,
                        "pipeline_id": event.pipeline_id,
                        "pool": event.pool,
                        "data": event.data,
                    }
                    for event in store.events(run_id=resolve_run_id(store, run_id), limit=limit)
                ]
            )
            return 200, {"rows": rows, "limit": limit}

        if path == "/pipelines":
            state = (query.get("state") or [None])[0]

            def _pipelines(store: Any) -> Any:
                rows = []
                selected = resolve_run_id(store, run_id)
                for record in store.pipelines(run_id=selected, state=state, limit=limit):
                    # `handoffs` costs one small indexed read per returned row, and is how this view says
                    # "the cursor below is a position, not progress": a pipeline with handoffs skipped
                    # stations, so n_tasks_done is where it is, not how many tasks ran.
                    count = getattr(store, "handoffs", None)
                    history = count(pipeline_id=record.pipeline_id) if callable(count) else []
                    handoffs = sum((hop.handoff_id or 0) > record.handoff_floor for hop in history)
                    visit_reader = getattr(store, "visit_state", None)
                    traversal = visit_reader(record.pipeline_id) if callable(visit_reader) else None
                    rows.append(
                        {
                            "pipeline_id": record.pipeline_id,
                            "suite_id": record.suite_id, "experiment_id": record.experiment_id,
                            "local_key": record.local_key, "repeat": record.repeat,
                            "name": record.name,
                            "state": record.state,
                            "run_id": record.run_id,
                            "n_tasks_done": record.n_tasks_done,
                            "n_tasks_total": record.n_tasks_total,
                            "handoffs": handoffs,
                            "handoffs_historical": len(history),
                            **({"control": traversal, "cursor_kind": "position"} if traversal is not None else {}),
                            "handoff_floor": record.handoff_floor,
                            "attempts_total": record.attempts_total,
                            "failed_task": record.failed_task,
                            "error_type": record.error_type,
                            "error_message": record.error_message,
                            "started_at": record.started_at,
                            "finished_at": record.finished_at,
                            "reported_metrics": [
                                {"name": item.name, "value": item.value, "label": item.label,
                                 "display": item.display, "updated_at": item.updated_at}
                                for item in read_reported_metrics(
                                    store, run_id=record.run_id, pipeline_id=record.pipeline_id
                                )
                            ],
                        }
                    )
                return rows

            rows = read(_pipelines)
            return 200, {"rows": rows, "limit": limit}

        if path == "/resources":
            pool = (query.get("pool") or [None])[0]

            def _resources(store: Any) -> Any:
                # resources() is an optional capability, not part of the Store protocol (see
                # store/base.py): a custom Store that predates it should degrade, not 500.
                fn = getattr(store, "resources", None)
                if fn is None:
                    return None
                return fn(pool=pool)

            rows = self._read(_resources)
            if rows is None:
                return 200, {"rows": [], "note": "this store backend does not persist resource state"}
            return 200, {"rows": rows}

        if path == "/errors":
            return 200, {"rows": read(lambda store: store.errors(
                run_id=resolve_run_id(store, run_id), limit=limit
            ))}

        return 404, {"error": f"unknown path {path!r}", "paths": _PATHS}


_PATHS = ["/", "/stats", "/metrics", "/events", "/pipelines", "/resources", "/errors", "/healthz"]


def _int(values: list[str] | None, default: int) -> int:
    if not values:
        return default
    try:
        return max(1, min(10000, int(values[0])))
    except (TypeError, ValueError):
        return default


def _make_handler(server: StatsServer) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "pyattacker"

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            try:
                status, body = server.payload(parsed.path, parse_qs(parsed.query))
            except Exception as exc:  # a broken store must not kill the server thread
                status, body = 500, {"error": f"{type(exc).__name__}: {exc}"}
            if "_html" in body:
                self._respond(status, body["_html"].encode("utf-8"), "text/html; charset=utf-8")
                return
            payload = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
            self._respond(status, payload, "application/json; charset=utf-8")

        def _respond(self, status: int, payload: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, fmt: str, *args: Any) -> None:  # keep stderr quiet by default
            return

    return Handler


def serve(
    store: Store | str,
    *,
    host: str = "127.0.0.1",
    port: int = 8787,
    run_id: str | None = None,
    block: bool = True,
) -> StatsServer:
    """Start the endpoint; by default block until Ctrl-C (what the CLI wants)."""
    server = StatsServer(store, host=host, port=port, run_id=run_id).start()
    if not block:
        return server
    try:
        server.wait()  # pragma: no cover - interactive path
    except KeyboardInterrupt:  # pragma: no cover
        pass
    finally:
        server.stop()
    return server
