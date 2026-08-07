"""An in-process stand-in for Solr's Core-Admin and update handlers.

Only the handful of endpoints the ingester talks to are implemented, but the
one detail that matters for blue/green rotation is modelled faithfully: a core
is a *name* pointing at an *instance directory*, the documents live with the
directory, and ``CREATE`` derives the directory from the name. That is what
makes ``SWAP`` move the live core into the suffixed directory and what makes a
second ``CREATE`` of an already rotated suffix fail - the production incident
this harness exists to reproduce.

Using stdlib ``http.server`` (rather than an aiohttp test server) keeps the
fake off the ingester's event loop, so the public, synchronous ``index()``
entry point can be exercised unchanged.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

SOLR_HOME = "/data/db"


class SolrState:
    """The mutable state of the fake: cores, their dirs and their documents."""

    def __init__(self) -> None:
        self.cores: Dict[str, str] = {}  # name -> instance dir basename
        self.docs: Dict[str, List[Dict[str, Any]]] = {}  # dir -> documents
        self.reject_updates: Optional[int] = None  # status to answer with
        self.reject_limit: Optional[int] = None  # reject only the first N
        self.rejected: int = 0
        self.fail_swap_after: Optional[int] = None  # let N swaps through first
        self.swaps: int = 0
        self.fail_create: bool = False
        self.fail_rename: bool = False
        self.fail_unload: bool = False
        self.created: List[str] = []
        self.unloaded: List[str] = []

    # -- helpers used by the tests ----------------------------------------- #
    def seed(self, name: str, docs: Optional[List[Dict[str, Any]]] = None) -> None:
        """Register a live core, optionally pre-filled."""
        self.cores[name] = name
        self.docs[name] = list(docs or [])

    def num_docs(self, name: str) -> int:
        return len(self.docs.get(self.cores[name], []))

    def documents(self, name: str) -> List[Dict[str, Any]]:
        return list(self.docs.get(self.cores[name], []))

    def instance_dir(self, name: str) -> str:
        return self.cores[name]

    # -- core admin --------------------------------------------------------- #
    def create(self, name: str) -> Tuple[int, Dict[str, Any]]:
        if self.fail_create:
            return 400, {"error": {"msg": f"configset for '{name}' is broken"}}
        if name in self.cores:
            return 400, {"error": {"msg": f"Core with name '{name}' already exists."}}
        if name in self.docs or name in self.cores.values():
            # Solr derives the instance dir from the name, so a directory held
            # by a *differently named* core blocks the create.
            return 400, {
                "error": {
                    "msg": (
                        f"Error CREATEing SolrCore '{name}': Could not create a "
                        f"new core in {SOLR_HOME}/{name} as another core is "
                        "already defined there"
                    ),
                    "code": 400,
                }
            }
        self.cores[name] = name
        self.docs[name] = []
        self.created.append(name)
        return 200, {"core": name}

    def swap(self, one: str, other: str) -> Tuple[int, Dict[str, Any]]:
        self.swaps += 1
        if self.fail_swap_after is not None and self.swaps > self.fail_swap_after:
            return 500, {"error": {"msg": "swap boom"}}
        if one not in self.cores or other not in self.cores:
            return 400, {"error": {"msg": "no such core"}}
        self.cores[one], self.cores[other] = self.cores[other], self.cores[one]
        return 200, {}

    def rename(self, core: str, other: str) -> Tuple[int, Dict[str, Any]]:
        if self.fail_rename:
            return 500, {"error": {"msg": "rename boom"}}
        if core not in self.cores:
            return 400, {"error": {"msg": f"no such core {core}"}}
        self.cores[other] = self.cores.pop(core)
        return 200, {}

    def unload(self, core: str, delete_dir: bool) -> Tuple[int, Dict[str, Any]]:
        if self.fail_unload:
            return 500, {"error": {"msg": "unload boom"}}
        if core not in self.cores:
            return 400, {"error": {"msg": f"no such core {core}"}}
        instance_dir = self.cores.pop(core)
        if delete_dir:
            self.docs.pop(instance_dir, None)
        self.unloaded.append(core)
        return 200, {}

    def status(self, core: Optional[str]) -> Dict[str, Any]:
        names = [core] if core else list(self.cores)
        out: Dict[str, Any] = {}
        for name in names:
            if name not in self.cores:
                # Solr answers 200 with an empty entry for an unknown core.
                out[name] = {}
                continue
            instance_dir = self.cores[name]
            out[name] = {
                "name": name,
                "instanceDir": f"{SOLR_HOME}/{instance_dir}/",
                "dataDir": f"{SOLR_HOME}/{instance_dir}/data/",
                "index": {"numDocs": len(self.docs.get(instance_dir, []))},
            }
        return {"status": out}


def _handler(state: SolrState) -> Any:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:  # keep the test output clean
            return

        def _send(self, status: int, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            query = {k: v[0] for k, v in parse_qs(url.query).items()}
            if url.path == "/solr/admin/cores":
                action = query.get("action", "").upper()
                if action == "STATUS":
                    self._send(200, state.status(query.get("core")))
                elif action == "CREATE":
                    self._send(*state.create(query["name"]))
                elif action == "SWAP":
                    self._send(*state.swap(query["core"], query["other"]))
                elif action == "RENAME":
                    self._send(*state.rename(query["core"], query["other"]))
                elif action == "UNLOAD":
                    delete_dir = query.get("deleteInstanceDir", "false") == "true"
                    self._send(*state.unload(query["core"], delete_dir))
                else:
                    self._send(400, {"error": {"msg": f"unknown action {action}"}})
                return
            parts = url.path.strip("/").split("/")
            if len(parts) == 3 and parts[0] == "solr" and parts[2] == "select":
                core = parts[1]
                if core not in state.cores:
                    self._send(404, {"error": {"msg": f"no such core {core}"}})
                    return
                num = len(state.docs.get(state.cores[core], []))
                self._send(200, {"response": {"numFound": num}})
                return
            self._send(404, {"error": {"msg": "not found"}})

        def do_POST(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"[]"
            parts = url.path.strip("/").split("/")
            if len(parts) == 4 and parts[2] == "update" and parts[3] == "json":
                core = parts[1]
                if core not in state.cores:
                    self._send(404, {"error": {"msg": f"no such core {core}"}})
                    return
                over_limit = (
                    state.reject_limit is not None
                    and state.rejected >= state.reject_limit
                )
                if state.reject_updates and raw not in (b"[]", b"") and not over_limit:
                    state.rejected += 1
                    self._send(
                        state.reject_updates,
                        {"error": {"msg": "unknown field 'nope'", "code": 400}},
                    )
                    return
                payload = json.loads(raw or b"[]")
                if isinstance(payload, dict) and "delete" in payload:
                    state.docs[state.cores[core]] = []
                elif isinstance(payload, list):
                    state.docs.setdefault(state.cores[core], []).extend(payload)
                self._send(200, {"responseHeader": {"status": 0}})
                return
            self._send(404, {"error": {"msg": "not found"}})

    return Handler


def serve(state: SolrState) -> Iterator[str]:
    """Run ``state`` behind a throwaway HTTP server; yield its base url."""

    class Server(ThreadingHTTPServer):
        daemon_threads = True

        def handle_error(self, request: Any, client_address: Any) -> None:
            """Client-side disconnects are normal here and only add noise."""
            return

    server = Server(("127.0.0.1", 0), _handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
