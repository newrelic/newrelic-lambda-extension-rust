#!/usr/bin/env python3
"""
Mock New Relic endpoint for RIE integration testing.

Tracks every POST to /log/v1 and can be configured to:
  - fail the first N requests  (fail_first_n)
  - always fail                 (always_fail)
  - randomly fail X % of reqs  (random_fail_pct, 0–100)
  - hang for M ms before reply  (hang_for_ms)  ← triggers extension 2.4 s timeout

Control endpoints:
  GET  /stats       — counters: total_requests, total_logs, unique_messages,
                      timed_out_requests, failed_requests
  GET  /messages    — flat list of every received log message (in order)
  GET  /log_numbers — parsed log numbers from "[prefix] log NNN" messages,
                      so you can see which numbered logs arrived vs were missed
  GET  /requests    — full raw batches
  POST /reset       — wipe all state
  POST /config      — change behaviour at runtime:
                      {"fail_first_n": 3}
                      {"always_fail": true}
                      {"random_fail_pct": 50}         ← fail ~50 % of requests
                      {"hang_for_ms": 5000}            ← hang 5 s (extension 2.4 s timeout fires)
                      {"hang_for_ms": 0}               ← disable hang
                      {"hang_on_requests": [1, 3, 5]}  ← hang only on specific request numbers

Env vars:
  FAIL_FIRST_N=N      return 500 for first N POSTs  (default 0)
  ALWAYS_FAIL=1       always return 500
  RANDOM_FAIL_PCT=50  randomly fail 50 % of requests
  HANG_FOR_MS=0       hang this many ms before every response
  PORT=9999           listen port
"""

import gzip
import json
import os
import random
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

_lock = threading.Lock()
_state = {
    "received":          [],    # [{ts, request_num, body}]
    "fail_first_n":      int(os.environ.get("FAIL_FIRST_N", "0")),
    "always_fail":       os.environ.get("ALWAYS_FAIL", "").lower() in ("1", "true", "yes"),
    "random_fail_pct":   int(os.environ.get("RANDOM_FAIL_PCT", "0")),
    "hang_for_ms":       int(os.environ.get("HANG_FOR_MS", "0")),
    "hang_on_requests":  [],    # if non-empty, only hang on these request numbers
    "request_count":     0,     # total POSTs to /log/v1 (including failures / hangs)
    "failed_requests":   0,
    "timed_out_requests": 0,    # hangs that the client gave up on
}


# ─── helpers ──────────────────────────────────────────────────────────────────

def _count_logs(received):
    total = 0
    for entry in received:
        for group in entry.get("body", []):
            total += len(group.get("logs", []))
    return total


def _extract_messages(received):
    msgs = []
    for entry in received:
        for group in entry.get("body", []):
            for log in group.get("logs", []):
                msgs.append(log.get("message", ""))
    return msgs


def _extract_log_numbers(received):
    """
    Parse '[prefix] log NNN  req=REQID' messages and group by prefix.

    Returns a dict keyed by prefix:
      {
        "inv-1": {
          "received": [1, 2, 3, ...],
          "missing":  [7, 8],         # gaps in the sequence
          "received_count": 98,
          "missing_count":  2,
        },
        ...
      }
    """
    import re
    pattern = re.compile(r"\[([^\]]+)\] log (\d+)")
    groups: dict = {}
    for msg in _extract_messages(received):
        m = pattern.search(msg)
        if m:
            prefix = m.group(1)
            num    = int(m.group(2))
            groups.setdefault(prefix, []).append(num)

    result = {}
    for prefix, nums in groups.items():
        nums_sorted = sorted(nums)
        full        = list(range(1, nums_sorted[-1] + 1)) if nums_sorted else []
        missing     = sorted(set(full) - set(nums_sorted))
        result[prefix] = {
            "received_count": len(nums_sorted),
            "received":       nums_sorted,
            "missing_count":  len(missing),
            "missing":        missing,
        }
    return result


def _should_fail(count):
    if _state["always_fail"]:
        return True, "always_fail"
    if count <= _state["fail_first_n"]:
        return True, f"fail_first_n={_state['fail_first_n']}"
    if _state["random_fail_pct"] > 0 and random.randint(1, 100) <= _state["random_fail_pct"]:
        return True, f"random_fail_pct={_state['random_fail_pct']}%"
    return False, ""


# ─── HTTP handler ─────────────────────────────────────────────────────────────

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence default access log; we print our own

    # ------------------------------------------------------------------ GET --
    def do_GET(self):
        if self.path == "/stats":
            with _lock:
                msgs = _extract_messages(_state["received"])
                s = {
                    "total_requests":      _state["request_count"],
                    "failed_requests":     _state["failed_requests"],
                    "timed_out_requests":  _state["timed_out_requests"],
                    "batches_received":    len(_state["received"]),
                    "total_logs":          _count_logs(_state["received"]),
                    "unique_messages":     len(set(msgs)),
                    "fail_first_n":        _state["fail_first_n"],
                    "always_fail":         _state["always_fail"],
                    "random_fail_pct":     _state["random_fail_pct"],
                    "hang_for_ms":         _state["hang_for_ms"],
                }
            self._json(200, s)

        elif self.path == "/messages":
            with _lock:
                msgs = _extract_messages(_state["received"])
            self._json(200, msgs)

        elif self.path == "/log_numbers":
            with _lock:
                by_prefix = _extract_log_numbers(_state["received"])
            # Summary totals across all prefixes
            total_received = sum(v["received_count"] for v in by_prefix.values())
            total_missing  = sum(v["missing_count"]  for v in by_prefix.values())
            self._json(200, {
                "total_received": total_received,
                "total_missing":  total_missing,
                "by_prefix":      by_prefix,
            })

        elif self.path == "/requests":
            with _lock:
                data = list(_state["received"])
            self._json(200, data)

        else:
            self.send_response(404)
            self.end_headers()

    # ----------------------------------------------------------------- POST --
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        # The extension gzips payloads above a size threshold (src/newrelic/client.rs)
        # and marks them with Content-Encoding: gzip — decompress before json.loads.
        if self.headers.get("Content-Encoding", "").lower() == "gzip":
            try:
                raw = gzip.decompress(raw)
            except OSError:
                pass  # leave raw as-is; downstream json.loads falls back gracefully

        # ── control endpoints ────────────────────────────────────────────────
        if self.path == "/reset":
            with _lock:
                _state["received"].clear()
                _state["request_count"]      = 0
                _state["failed_requests"]    = 0
                _state["timed_out_requests"] = 0
            print("[mock-nr] State reset", flush=True)
            self.send_response(200)
            self.end_headers()
            return

        if self.path == "/config":
            try:
                cfg = json.loads(raw)
                with _lock:
                    if "fail_first_n"    in cfg: _state["fail_first_n"]    = int(cfg["fail_first_n"])
                    if "always_fail"     in cfg: _state["always_fail"]     = bool(cfg["always_fail"])
                    if "random_fail_pct" in cfg: _state["random_fail_pct"] = int(cfg["random_fail_pct"])
                    if "hang_for_ms"      in cfg: _state["hang_for_ms"]      = int(cfg["hang_for_ms"])
                    if "hang_on_requests" in cfg: _state["hang_on_requests"] = list(cfg["hang_on_requests"])
                    print(
                        f"[mock-nr] Config → fail_first_n={_state['fail_first_n']} "
                        f"always_fail={_state['always_fail']} "
                        f"random_fail_pct={_state['random_fail_pct']}% "
                        f"hang_for_ms={_state['hang_for_ms']}ms "
                        f"hang_on_requests={_state['hang_on_requests']}",
                        flush=True,
                    )
                self.send_response(200)
            except Exception as exc:
                print(f"[mock-nr] Bad config: {exc}", flush=True)
                self.send_response(400)
            self.end_headers()
            return

        # ── NR log endpoint ──────────────────────────────────────────────────
        if self.path in ("/log/v1", "/v1/logs"):
            with _lock:
                _state["request_count"] += 1
                count = _state["request_count"]
                hang_ms = _state["hang_for_ms"]
                hang_on_reqs = list(_state["hang_on_requests"])
                fail, reason = _should_fail(count)
                if fail:
                    _state["failed_requests"] += 1

            # hang_on_requests narrows the hang to specific request numbers only
            effective_hang_ms = hang_ms if (not hang_on_reqs or count in hang_on_reqs) else 0

            # Hang BEFORE acquiring lock again — allows concurrent requests
            if effective_hang_ms > 0:
                print(
                    f"[mock-nr] #{count} POST /log/v1 → hanging {effective_hang_ms}ms "
                    f"(extension timeout = 2400ms)",
                    flush=True,
                )
                time.sleep(effective_hang_ms / 1000.0)
                # After sleep, check if the client is still connected
                try:
                    if fail:
                        self._json(500, {"error": "intentional failure after hang"})
                    else:
                        with _lock:
                            try:
                                body = json.loads(raw)
                            except Exception:
                                body = raw.decode(errors="replace")
                            _state["received"].append({
                                "ts":          datetime.now(timezone.utc).isoformat(),
                                "request_num": count,
                                "body":        body,
                            })
                            log_lines = _count_logs(_state["received"][-1:])
                            total     = _count_logs(_state["received"])
                        print(
                            f"[mock-nr] #{count} POST /log/v1 → 202 after {effective_hang_ms}ms hang "
                            f"(+{log_lines} logs, total={total})",
                            flush=True,
                        )
                        self._json(202, {"requestId": f"mock-{count}"})
                except (BrokenPipeError, ConnectionResetError):
                    with _lock:
                        _state["timed_out_requests"] += 1
                    print(
                        f"[mock-nr] #{count} client disconnected after {effective_hang_ms}ms hang "
                        f"(extension timed out — log was NOT saved)",
                        flush=True,
                    )
                return

            # No hang path
            if fail:
                print(
                    f"[mock-nr] #{count} POST /log/v1 → 500  reason={reason}",
                    flush=True,
                )
                self._json(500, {"error": reason})
            else:
                with _lock:
                    try:
                        body = json.loads(raw)
                    except Exception:
                        body = raw.decode(errors="replace")
                    _state["received"].append({
                        "ts":          datetime.now(timezone.utc).isoformat(),
                        "request_num": count,
                        "body":        body,
                    })
                    log_lines = _count_logs(_state["received"][-1:])
                    total     = _count_logs(_state["received"])
                print(
                    f"[mock-nr] #{count} POST /log/v1 → 202  (+{log_lines} logs, total={total})",
                    flush=True,
                )
                self._json(202, {"requestId": f"mock-{count}"})
            return

        # ── NR telemetry endpoint — always accept ────────────────────────────
        if self.path == "/aws/lambda/v1":
            self._json(200, {"status": "ok"})
            return

        self.send_response(200)
        self.end_headers()

    # ─────────────────────────────────────────────────────────────────────────
    def _json(self, status, obj):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    port = int(os.environ.get("PORT", "9999"))
    server = HTTPServer(("0.0.0.0", port), _Handler)
    print(
        f"[mock-nr] Listening on :{port}  "
        f"fail_first_n={_state['fail_first_n']}  "
        f"always_fail={_state['always_fail']}  "
        f"random_fail_pct={_state['random_fail_pct']}%  "
        f"hang_for_ms={_state['hang_for_ms']}ms  "
        f"hang_on_requests={_state['hang_on_requests']}",
        flush=True,
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
