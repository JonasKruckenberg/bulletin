#!/usr/bin/env python3
"""Minimal stand-in for `llama-server`, used only by the NixOS egress test.

Shipping the real llama.cpp + a multi-GB GGUF in a sandboxed VM test is
impractical, but the thing under test — the cgroup network policy the module
wires onto the `llama-cpp` unit — is pure systemd plumbing that any process in
that unit exercises. So this fake ignores every llama.cpp flag except
`--host`/`--port`, binds the loopback address the real sidecar would, and
exposes two probes that run *inside the unit's confinement*:

  GET /probe/egress?target=HOST:PORT   attempt an outbound TCP connect
  GET /probe/loopback                  attempt a TCP connect to itself (loopback)

The test reads the JSON result to assert egress is blocked (the worker's data
never leaves the box) while the loopback path the worker uses still works.
"""
import json
import socket
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

host = "127.0.0.1"
port = 8080
argv = sys.argv[1:]
for i, a in enumerate(argv):
    if a == "--host" and i + 1 < len(argv):
        host = argv[i + 1]
    elif a == "--port" and i + 1 < len(argv):
        port = int(argv[i + 1])


def probe(target_host, target_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    try:
        s.connect((target_host, target_port))
        return {"ok": True, "errno": None, "error": None}
    except OSError as e:
        # A cgroup IPAddressDeny drop surfaces as EPERM (1) / EACCES (13) at
        # connect() time; anything else (timeout, refused) means the policy was
        # *not* what stopped us — the test distinguishes the two.
        return {"ok": False, "errno": e.errno, "error": str(e)}
    finally:
        try:
            s.close()
        except OSError:
            pass


class Handler(BaseHTTPRequestHandler):
    def _reply(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/probe/egress":
            target = parse_qs(u.query).get("target", ["127.0.0.1:0"])[0]
            target_host, _, target_port = target.rpartition(":")
            self._reply(probe(target_host, int(target_port)))
        elif u.path == "/probe/loopback":
            self._reply(probe("127.0.0.1", port))
        else:
            self._reply({"ok": True})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        self._reply({"ok": True})

    def log_message(self, *args):
        pass


HTTPServer((host, port), Handler).serve_forever()
