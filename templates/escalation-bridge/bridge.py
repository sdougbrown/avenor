#!/usr/bin/env python3
"""Reference bridge for Avenor's remote-human gate escalation surface.

Watches a durable workflow for parked `awaiting_gate` activations, carries each
pending human gate to a transport, and records the human's answer through the
ordinary gate command path. Avenor ships no notification code; this bridge is
the missing transport half, kept deliberately generic:

  - ask:     POSTs a JSON payload to --webhook-url (Challah, a Slack bot, a
             pager script — anything that can receive a webhook)
  - answer:  polls --decision-dir for `decision-<gate_id>.json` files written
             by the transport

See docs/escalation.md for the protocol contract and the actor/evidence
conventions. Stdlib only; Python 3.9+.

Usage:
  python3 bridge.py --socket /tmp/avenor-stable.sock --workflow-id wf_... \
      --webhook-url http://127.0.0.1:8765/ask --decision-dir /tmp/escalation
"""

import argparse
import hashlib
import json
import socket
import sys
import time
import urllib.request
from pathlib import Path


class Control:
    """Minimal newline-delimited JSON-RPC 2.0 client for an Avenor control socket."""

    def __init__(self, path):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(path)
        self.sockfile = self.sock.makefile("rw")
        self.next_id = 0

    def call(self, method, params=None, timeout=35.0):
        self.next_id += 1
        req = {"jsonrpc": "2.0", "id": self.next_id, "method": method}
        if params is not None:
            req["params"] = params
        self.sock.settimeout(timeout)
        self.sockfile.write(json.dumps(req) + "\n")
        self.sockfile.flush()
        deadline = time.monotonic() + timeout
        for line in self.sockfile:
            resp = json.loads(line)
            if resp.get("id") == req["id"]:
                if "error" in resp:
                    raise RuntimeError(f"{method}: {resp['error']}")
                return resp.get("result")
            if time.monotonic() > deadline:
                raise TimeoutError(method)
        raise ConnectionError("control socket closed")


def pending_human_gates(result):
    """Extract (node_id, activation_id, gate, subject) for each unsatisfied human gate."""
    found = []
    for node in result.get("nodes", []):
        for act in node.get("activations", []):
            if act.get("status") != "awaiting_gate":
                continue
            subject = act.get("subject")
            for gate in node.get("gates", []):
                if gate.get("type") == "human" and gate.get("status") in (None, "pending"):
                    found.append((node["id"], act["id"], gate, subject))
    return found


def ask(webhook_url, payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        webhook_url, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--socket", required=True, help="Avenor stable control socket path")
    ap.add_argument("--workflow-id", required=True)
    ap.add_argument("--webhook-url", required=True, help="Transport endpoint for the question payload")
    ap.add_argument("--decision-dir", required=True, help="Directory the transport writes decision files into")
    ap.add_argument("--timeout", default="30s", help="workflow.wait timeout per iteration (default 30s)")
    args = ap.parse_args()

    timeout_ms = int(args.timeout.rstrip("s")) * 1000
    decisions = Path(args.decision_dir)
    decisions.mkdir(parents=True, exist_ok=True)

    ctl = Control(args.socket)
    print(f"bridge: watching {args.workflow_id} on {args.socket}", flush=True)

    while True:
        status = ctl.call("workflow.wait", {"workflow_id": args.workflow_id, "timeout_ms": timeout_ms})
        status = status or {}
        if status.get("status") in ("completed", "failed", "abandoned"):
            print(f"bridge: workflow terminal ({status.get('status')})", flush=True)
            return 0
        if status.get("status") != "awaiting_gate":
            continue

        detail = ctl.call("workflow.inspect", {"workflow_id": args.workflow_id})
        for node_id, activation_id, gate, subject in pending_human_gates(detail):
            question = {
                "workflow_id": args.workflow_id,
                "node_id": node_id,
                "activation_id": activation_id,
                "gate_id": gate["id"],
                "gate_name": gate.get("name", gate["id"]),
                "allowed_outcomes": gate.get("allowed_outcomes", []),
                "subject": subject,
                # The human must be able to decide from the message alone.
                "note": "Include what is being authorized, its exact scope, and what denial means.",
            }
            print(f"bridge: asking about gate {gate['id']} on {node_id}", flush=True)
            ask(args.webhook_url, question)

            # The transport writes decision-<gate_id>.json when the human answers:
            # {"decision": "satisfy" | "reject", "actor": "...", "reason": "..."}
            decision_file = decisions / f"decision-{gate['id']}.json"
            while not decision_file.exists():
                time.sleep(2)
            decision = json.loads(decision_file.read_text())
            decision_file.unlink()

            response_hash = hashlib.sha256(
                json.dumps(decision, sort_keys=True).encode()
            ).hexdigest()[:32]
            ctl.call(
                "workflow.command",
                {
                    "workflow_id": args.workflow_id,
                    "command": {
                        "op": "gate",
                        "node_id": node_id,
                        "activation_id": activation_id,
                        "gate_id": gate["id"],
                        "operation": decision["decision"],
                        "actor": decision["actor"],
                        "reason": decision["reason"],
                        "source": "escalation-bridge",
                        "response_hash": response_hash,
                        **({"subject": subject} if subject else {}),
                    },
                },
            )
            print(f"bridge: recorded {decision['decision']} by {decision['actor']}", flush=True)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
