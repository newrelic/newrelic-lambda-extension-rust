"""
Minimal mock Lambda runtime for extension integration testing.

Replaces the AWS Lambda RIE for local testing. Implements the Extensions API
and Telemetry API endpoints that the extension needs, and ACTUALLY forwards
telemetry events to registered subscribers (unlike the open-source RIE which
stubs out the Telemetry API with "not supported").

Exposes two ports:
  9001  — AWS_LAMBDA_RUNTIME_API  (Extensions + Telemetry API)
  9000  — Test invocation trigger  (matches RIE's public invoke port)

Test script invocation format (POST /2015-03-31/functions/function/invocations):
  {"log_prefix": "inv-1", "log_count": 5}

This generates synthetic function log entries and forwards them to any
registered telemetry subscriber, simulating what the real Lambda platform does.

Lifecycle per invocation:
  1. POST /invoke arrives (test script)
  2. Queue an INVOKE event (unblocks a waiting /next call from the extension)
  3. Generate log events and POST them to telemetry subscriber
  4. Extension calls flush() → sends logs to mock NR → calls /next again
  5. Next invoke starts the cycle again
"""

import json
import logging
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from urllib import request as urllib_request, error as urllib_error

logging.basicConfig(
    format="[mock-runtime] %(message)s",
    level=logging.DEBUG,
)
log = logging.getLogger("mock-runtime")

# ── State ────────────────────────────────────────────────────────────────────
_lock = threading.Lock()
_extensions: dict[str, str] = {}          # ext_id → name
_next_queue: list[dict] = []              # pending events for /next
_next_events = threading.Event()          # signals that _next_queue is non-empty
_telemetry_subscribers: list[str] = []    # destination URIs
_function_arn = "arn:aws:lambda:us-east-1:012345678912:function:rie-test-function"
_current_request_id: str = ""


def _make_invoke_event(request_id: str) -> dict:
    return {
        "eventType": "INVOKE",
        "deadlineMs": int(time.time() * 1000) + 30_000,
        "requestId": request_id,
        "invokedFunctionArn": _function_arn,
        "tracing": {"type": "X-Amzn-Trace-Id", "value": "Root=1-test;Sampled=0"},
    }


def _make_shutdown_event() -> dict:
    return {
        "eventType": "SHUTDOWN",
        "shutdownReason": "spindown",
        "deadlineMs": int(time.time() * 1000) + 2_000,
    }


def _make_platform_start(request_id: str) -> list[dict]:
    return [{
        "time": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "type": "platform.start",
        "record": {
            "requestId": request_id,
            "version": "$LATEST",
            "tracing": {"spanId": "test-span", "type": "X-Amzn-Trace-Id",
                        "value": "Root=1-test;Sampled=0"},
        },
    }]


def _make_function_logs(request_id: str, prefix: str, count: int) -> list[dict]:
    ts = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    events = []
    for i in range(1, count + 1):
        # Plain numbered log — matches handler.py format so /log_numbers gap
        # detection works by prefix ("s1a", "s2b", etc.).
        msg = f"[{prefix}] log {i:03d}  req={request_id[:8]}"
        events.append({
            "time": ts,
            "type": "function",
            "record": {
                "timestamp": ts,
                "message": msg,
            },
        })
    return events


def _make_extension_logs(request_id: str, prefix: str, count: int) -> list[dict]:
    ts = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
    events = []
    for i in range(1, count + 1):
        msg = f"[{prefix}] log {i:03d}  req={request_id[:8]}"
        events.append({
            "time": ts,
            "type": "extension",
            "record": {
                "timestamp": ts,
                "message": msg,
            },
        })
    return events


def _make_runtime_done(request_id: str) -> list[dict]:
    return [{
        "time": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "type": "platform.runtimeDone",
        "record": {
            "requestId": request_id,
            "status": "success",
        },
    }]


def _forward_telemetry(events: list[dict]) -> None:
    """POST telemetry events to all registered subscribers."""
    with _lock:
        subscribers = list(_telemetry_subscribers)
    if not subscribers or not events:
        return
    payload = json.dumps(events).encode()
    for uri in subscribers:
        try:
            req = urllib_request.Request(
                uri,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib_request.urlopen(req, timeout=5) as resp:
                log.debug("Forwarded %d telemetry event(s) to %s → %d",
                          len(events), uri, resp.status)
        except urllib_error.URLError as e:
            log.warning("Failed to forward telemetry to %s: %s", uri, e)


# ── HTTP Handlers ────────────────────────────────────────────────────────────

class RuntimeAPIHandler(BaseHTTPRequestHandler):
    """Handles AWS_LAMBDA_RUNTIME_API calls from the extension (port 9001)."""

    def log_message(self, fmt, *args):
        log.debug("RUNTIME %s - %s", self.path, fmt % args)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _send_json(self, status: int, data) -> None:
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ── Extensions API ────────────────────────────────────────────────────────

    def do_POST(self):
        if self.path == "/2020-01-01/extension/register":
            body = json.loads(self._read_body() or b"{}")
            ext_id = str(uuid.uuid4())
            name = body.get("functionName", "unknown")
            with _lock:
                _extensions[ext_id] = name
            log.info("Extension registered: %s  id=%s", name, ext_id)
            self.send_response(200)
            self.send_header("Lambda-Extension-Identifier", ext_id)
            self.send_header("Content-Type", "application/json")
            body_out = json.dumps({
                "functionName": _function_arn.split(":")[-1],
                "functionVersion": "$LATEST",
                "handler": "handler.handler",
            }).encode()
            self.send_header("Content-Length", str(len(body_out)))
            self.end_headers()
            self.wfile.write(body_out)

        elif self.path == "/2020-01-01/extension/init/error":
            self._send_json(202, {})

        elif self.path == "/2020-01-01/extension/exit/error":
            self._send_json(202, {})

        else:
            self._send_json(404, {"error": f"Unknown POST path: {self.path}"})

    def do_GET(self):
        if self.path == "/2020-01-01/extension/event/next":
            ext_id = self.headers.get("Lambda-Extension-Identifier", "unknown")
            log.debug("/next called by ext %s — waiting for event", ext_id)
            # Block until an event is queued (with timeout)
            while True:
                _next_events.wait(timeout=300)
                with _lock:
                    if _next_queue:
                        event = _next_queue.pop(0)
                        if not _next_queue:
                            _next_events.clear()
                        break
                _next_events.clear()
            log.debug("/next returning: %s", event.get("eventType"))
            self._send_json(200, event)
        else:
            self._send_json(404, {"error": f"Unknown GET path: {self.path}"})

    def do_PUT(self):
        if self.path.startswith("/2022-07-01/telemetry"):
            body = json.loads(self._read_body() or b"{}")
            dest = body.get("destination", {})
            uri = dest.get("URI", "")
            if uri:
                with _lock:
                    if uri not in _telemetry_subscribers:
                        _telemetry_subscribers.append(uri)
                        log.info("Telemetry subscriber registered: %s", uri)
            self._send_json(200, {})
        else:
            self._send_json(404, {"error": f"Unknown PUT path: {self.path}"})


class InvokeAPIHandler(BaseHTTPRequestHandler):
    """Handles test invocations (port 9000) — mirrors the RIE's public API."""

    def log_message(self, fmt, *args):
        log.debug("INVOKE  %s - %s", self.path, fmt % args)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _send_json(self, status: int, data) -> None:
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/2015-03-31/functions/function/invocations":
            self._send_json(404, {"error": "Not found"})
            return

        body = json.loads(self._read_body() or b"{}")
        log_prefix = body.get("log_prefix", "test")
        log_count = int(body.get("log_count", 5))
        ext_log_count = int(body.get("ext_log_count", 0))
        request_id = str(uuid.uuid4())

        global _current_request_id
        _current_request_id = request_id
        log.info("Invoke: prefix=%s count=%d ext_count=%d request_id=%s",
                 log_prefix, log_count, ext_log_count, request_id)

        # 1. Send HTTP response immediately — caller must not block on telemetry forwarding
        self._send_json(200, {
            "statusCode": 200,
            "body": json.dumps({
                "logs_emitted": log_count,
                "prefix": log_prefix,
                "request_id": request_id,
            }),
        })

        # 2. Queue INVOKE event and forward telemetry in background so we don't
        #    deadlock the HTTP handler thread (extension's /next is served by the
        #    same ThreadingHTTPServer pool).
        def _trigger(req_id: str, prefix: str, count: int, ext_count: int) -> None:
            ext_events = (
                _make_extension_logs(req_id, prefix + "-ext", ext_count)
                if ext_count > 0 else []
            )
            telemetry_events = (
                _make_platform_start(req_id)
                + _make_function_logs(req_id, prefix, count)
                + ext_events
                + _make_runtime_done(req_id)
            )
            with _lock:
                _next_queue.append(_make_invoke_event(req_id))
            _next_events.set()
            time.sleep(0.05)  # let extension see INVOKE before telemetry arrives
            _forward_telemetry(telemetry_events)

        threading.Thread(
            target=_trigger,
            args=(request_id, log_prefix, log_count, ext_log_count),
            daemon=True,
        ).start()


# ── Server startup ────────────────────────────────────────────────────────────

def _serve(server_class, handler_class, port: int, name: str) -> None:
    server = server_class(("0.0.0.0", port), handler_class)
    log.info("%s listening on :%d", name, port)
    server.serve_forever()


if __name__ == "__main__":
    threads = [
        threading.Thread(
            target=_serve,
            args=(ThreadingHTTPServer, RuntimeAPIHandler, 9001, "RuntimeAPI"),
            daemon=True,
        ),
        threading.Thread(
            target=_serve,
            args=(ThreadingHTTPServer, InvokeAPIHandler, 9000, "InvokeAPI"),
            daemon=True,
        ),
    ]
    for t in threads:
        t.start()

    log.info("Mock Lambda runtime ready (RuntimeAPI=9001, InvokeAPI=9000)")

    # Wait for shutdown
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutting down")
        with _lock:
            _next_queue.append(_make_shutdown_event())
        _next_events.set()
        time.sleep(1)
