#!/usr/bin/env python3
"""Reference bridge for Avenor's remote-human gate escalation surface.

Watches a durable workflow for parked `awaiting_gate` activations, carries each
pending human gate to a transport, and records the human's answer through the
ordinary gate command path. Avenor ships no notification code; this bridge is
the missing transport half, kept deliberately generic:

  - ask:     POSTs a JSON question payload to --webhook-url (Challah, a Slack
             bot, a pager script — anything that can receive a webhook)
  - answer:  polls --decision-dir for `decision-<activation>-<gate>.json`
             files written by the transport

Contract notes (see docs/escalation.md for the full protocol):

  - The kernel never moves the workflow status to `awaiting_gate`; parking is
    visible only on activations. `workflow.wait` returns on a terminal status
    (completed/failed/canceled) or on timeout, so the bridge treats every wait
    return as a tick and re-inspects.
  - Inspect carries flat `activations`/`gates`/`outputs` lists and no template
    definitions. Pending human gates are therefore derived from the parked
    activations plus the gate definitions loaded out-of-band from --template.
  - A recorded decision requires actor, reason, at least one evidence id, and
    — when the gate declares a subject_type — a subject whose type matches.
    The bridge derives a stable evidence id from the decision payload; the
    transport's durable pointer travels in the reason text.
  - A gate that declares subject_binding is decided against the subject the
    kernel pinned on the activation (inspect resolved_gates). The bridge reads
    that pinned subject, carries it in the question, and submits exactly it —
    never a transport-supplied or output-derived subject. An unresolved pin is
    skipped until it resolves; a subject that changes before submit drops the
    decision.

Stdlib only; Python 3.9+.

Usage:
  python3 bridge.py --socket /tmp/avenor-stable.sock --workflow-id wf_... \
      --template templates/escalation-bridge/demo.json \
      --webhook-url http://127.0.0.1:8765/ask --decision-dir /tmp/escalation
"""

import argparse
import hashlib
import itertools
import json
import socket
import sys
import time
import traceback
import urllib.request
from pathlib import Path

BACKOFF_SECONDS = 5.0


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
        for line in self.sockfile:
            resp = json.loads(line)
            if resp.get("id") == req["id"]:
                if "error" in resp:
                    raise RuntimeError(f"{method}: {resp['error']}")
                return resp.get("result")
        raise ConnectionError("control socket closed")


def parse_duration(text):
    """Parse '30s' / '5m' / '1h' into seconds."""
    if not text:
        raise ValueError("empty duration")
    units = {"s": 1, "m": 60, "h": 3600}
    unit = text[-1].lower()
    if unit not in units or not text[:-1].isdigit():
        raise ValueError(f"invalid duration {text!r} (expected e.g. 30s, 5m, 1h)")
    return int(text[:-1]) * units[unit]


def load_human_gates(template_path):
    """Map node_id -> [gate definition] for human gates declared in the template."""
    template = json.loads(Path(template_path).read_text())
    gates = {}
    for node in template.get("nodes", []):
        human = [g for g in node.get("gates", []) if g.get("type") == "human"]
        if human:
            gates[node["id"]] = human
    return gates


def pending_human_gates(detail, template_gates):
    """Yield (activation, gate_definition) for each parked human gate.

    Parking lives on activations (status awaiting_gate); a gate instance is
    recorded only when a decision lands, so a passed/waived instance means the
    gate no longer blocks this activation.
    """
    settled = {
        (g.get("activation_id"), g.get("gate_id"))
        for g in (detail.get("gates") or [])
        if g.get("status") in ("passed", "waived")
    }
    for act in (detail.get("activations") or []):
        if act.get("status") != "awaiting_gate":
            continue
        for gate in template_gates.get(act.get("node_id"), []):
            if not gate.get("required", False):
                continue
            if (act.get("activation_id"), gate["id"]) in settled:
                continue
            yield act, gate


def latest_outputs(detail):
    """Map definition_id -> decoded value, keeping the highest revision per output."""
    latest = {}
    for out in (detail.get("outputs") or []):
        oid = out.get("definition_id")
        rev = out.get("revision", 0)
        if oid is not None and rev >= latest.get(oid, (0, None))[0]:
            try:
                value = json.loads(out.get("value") or "null")
            except (TypeError, json.JSONDecodeError):
                value = out.get("value")
            latest[oid] = (rev, value)
    return {oid: value for oid, (_, value) in latest.items()}


def pinned_subject(activation, gate_id):
    """Return the gate's pinned subject from the activation's resolved_gates.

    A bound gate's subject is pinned onto the activation when it is created
    (resolved_gates[gate_id].subject). Returns the subject dict when resolved,
    or None when the gate is unbound, has no pin, or its pin is unresolved.
    """
    resolved = (activation.get("resolved_gates") or {}).get(gate_id)
    if not resolved:
        return None
    return resolved.get("subject")


def ask(webhook_url, payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        webhook_url, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def decision_response_hash(decision):
    return hashlib.sha256(
        json.dumps(decision, sort_keys=True).encode()
    ).hexdigest()[:32]


def build_gate_command(workflow_id, node_id, activation_id, gate, decision,
                      pinned_subject=None):
    """Build the workflow.command gate payload from the transport's decision.

    The kernel requires actor, reason, and at least one evidence id for
    satisfy/reject/waive, plus a subject with a non-empty type when the gate
    declares a subject_type. For a bound gate (pinned_subject supplied), the
    subject is exactly the one pinned on the activation — never the
    transport's. Evidence ids on a bridge-recorded decision are opaque audit
    references: the transport's durable pointer is appended to the reason so it
    survives in the append-only gate history.
    """
    decision = dict(decision)
    op = decision.get("decision")
    if op not in ("satisfy", "reject"):
        raise ValueError(f"decision must be 'satisfy' or 'reject', got {op!r}")
    actor = decision.get("actor")
    reason = decision.get("reason")
    if not actor or not reason:
        raise ValueError("decision requires 'actor' and 'reason'")

    if pinned_subject is not None:
        # Bound gate: submit exactly the subject pinned on the activation,
        # never one supplied by the transport or derived from latest outputs.
        subject = dict(pinned_subject)
    else:
        subject_type = gate.get("subject_type")
        subject = decision.get("subject")
        if subject_type:
            if not subject or subject.get("type") != subject_type:
                raise ValueError(
                    f"gate declares subject_type {subject_type!r}; decision.subject "
                    f"must carry a matching non-empty type"
                )

    evidence_pointer = decision.get("evidence")
    if evidence_pointer:
        reason = f"{reason}\nevidence: {evidence_pointer}"
    response_hash = decision_response_hash(decision)
    evidence_ids = ["ev_bridge_" + response_hash]

    command = {
        "op": "gate",
        "node_id": node_id,
        "activation_id": activation_id,
        "gate_id": gate["id"],
        "operation": op,
        "actor": actor,
        "reason": reason,
        "source": "escalation-bridge",
        "response_hash": response_hash,
        "evidence_ids": evidence_ids,
    }
    if subject:
        command["subject"] = subject
    if decision.get("outcome"):
        command["outcome"] = decision["outcome"]
    return command


def wait_for_decision(ctl, decisions, workflow_id, activation_id, gate, gate_timeout, poll):
    """Poll for the transport's decision file, re-checking workflow state.

    Returns (decision, decision_file, reason). reason is 'answered',
    'timeout' (gate-timeout expired while still parked; the caller re-asks),
    'unparked' (the activation resolved another way), or 'terminal' (the
    workflow reached a terminal state while waiting). The file is left in
    place for the caller to archive only after the decision is recorded
    kernel-side: a failed record must not consume the human's answer.
    """
    decision_file = decisions / f"decision-{activation_id}-{gate['id']}.json"
    deadline = time.monotonic() + gate_timeout
    while time.monotonic() < deadline:
        if decision_file.exists():
            try:
                return json.loads(decision_file.read_text()), decision_file, "answered"
            except json.JSONDecodeError as exc:
                # Malformed file: move it out of the way so the transport can
                # write a fresh one, and surface the parse error on the next
                # ask instead of looping on the same broken file.
                rejected = move_aside(decisions, activation_id, gate["id"], decision_file)
                return None, None, f"invalid decision file ({exc}); rewritten to {rejected.name}"
        time.sleep(poll)
        # Re-check state on every tick: another path may resolve the gate or
        # the workflow while this decision is pending.
        detail = ctl.call("workflow.inspect", {"workflow_id": workflow_id})
        act = next(
            (a for a in (detail.get("activations") or [])
             if a.get("activation_id") == activation_id),
            None,
        )
        if act is None or act.get("status") != "awaiting_gate":
            print(f"bridge: {activation_id} no longer parked; dropping wait on {gate['id']}", flush=True)
            return None, None, "unparked"
        if detail.get("instance", {}).get("status") in ("completed", "failed", "canceled"):
            return None, None, "terminal"
    return None, None, "timeout"


_move_aside_seq = itertools.count(1)


def move_aside(decisions, activation_id, gate_id, decision_file):
    """Park a decision file under rejected/ so it is never re-read.

    The name embeds a millisecond timestamp plus a per-process counter:
    whole-second timestamps would let two parks of the same activation+gate
    in one second overwrite each other.
    """
    rejected = decisions / "rejected" / (
        f"{activation_id}-{gate_id}-{int(time.time() * 1000)}-{next(_move_aside_seq)}.json"
    )
    rejected.parent.mkdir(parents=True, exist_ok=True)
    decision_file.rename(rejected)
    return rejected


def subject_unchanged(ctl, workflow_id, activation_id, gate_id, expected_subject):
    """Re-inspect before submitting a bound-gate decision.

    Confirms the activation is still awaiting_gate and its pinned subject is
    unchanged since the question was asked. Returns (ok, reason): reason is
    'ok', 'unparked', or 'subject changed'.
    """
    detail = ctl.call("workflow.inspect", {"workflow_id": workflow_id})
    act = next(
        (a for a in (detail.get("activations") or [])
         if a.get("activation_id") == activation_id),
        None,
    )
    if act is None or act.get("status") != "awaiting_gate":
        return False, "unparked"
    if pinned_subject(act, gate_id) != expected_subject:
        return False, "subject changed"
    return True, "ok"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--socket", required=True, help="Avenor stable control socket path")
    ap.add_argument("--workflow-id", required=True)
    ap.add_argument("--webhook-url", required=True, help="Transport endpoint for the question payload")
    ap.add_argument("--decision-dir", required=True, help="Directory the transport writes decision files into")
    ap.add_argument("--template", required=True, help="Template JSON the workflow was instantiated from")
    ap.add_argument("--wait", default="30s", help="workflow.wait timeout per tick (default 30s)")
    ap.add_argument("--gate-timeout", default="15m", help="Max wait per asked gate before re-asking (default 15m)")
    ap.add_argument("--poll", default="2s", help="Decision-file poll interval (default 2s)")
    args = ap.parse_args()

    wait_ms = parse_duration(args.wait) * 1000
    gate_timeout = parse_duration(args.gate_timeout)
    poll = max(parse_duration(args.poll), 1.0)
    decisions = Path(args.decision_dir)
    decisions.mkdir(parents=True, exist_ok=True)
    # The decision dir must stay local-user scoped: on a group- or
    # other-writable pre-existing dir, any local user could drop a
    # decision-*.json and forge an attributed decision (the actor is
    # self-asserted). Refuse to start on one; group/other read or execute
    # bits are fine — they cannot forge a decision.
    if decisions.stat().st_mode & 0o022:
        raise SystemExit(f"decision dir {decisions} must not be group- or other-writable")
    template_gates = load_human_gates(args.template)

    ctl = None
    decision_errors = {}
    print(f"bridge: watching {args.workflow_id} on {args.socket}", flush=True)
    while True:
        try:
            if ctl is None:
                ctl = Control(args.socket)
            result = ctl.call(
                "workflow.wait",
                {"workflow_id": args.workflow_id, "timeout_ms": wait_ms},
                timeout=wait_ms / 1000 + 10,
            ) or {}
            if result.get("terminal"):
                status = result.get("instance", {}).get("status")
                outcome = result.get("instance", {}).get("terminal_outcome", "")
                print(f"bridge: workflow terminal ({status}, outcome {outcome})", flush=True)
                return 0

            detail = ctl.call("workflow.inspect", {"workflow_id": args.workflow_id})
            for act, gate in pending_human_gates(detail, template_gates):
                node_id = act["node_id"]
                activation_id = act["activation_id"]
                gate_id = gate["id"]
                bound = gate.get("subject_binding") is not None
                pinned = pinned_subject(act, gate_id) if bound else None
                if bound and pinned is None:
                    # A bound gate whose subject has not resolved yet cannot be
                    # decided; skip it and let the next tick retry once the
                    # pin resolves.
                    print(
                        f"bridge: {gate_id} on {node_id} ({activation_id}) has "
                        f"an unresolved subject pin; skipping until resolved",
                        flush=True,
                    )
                    continue
                question = {
                    "workflow_id": args.workflow_id,
                    "node_id": node_id,
                    "activation_id": activation_id,
                    "gate_id": gate["id"],
                    "gate_name": gate.get("name", gate["id"]),
                    "subject_type": gate.get("subject_type"),
                    "allowed_outcomes": gate.get("allowed_outcomes", []),
                    "selected_outcome": act.get("selected_outcome"),
                    "outputs": latest_outputs(detail),
                    "decision_file": f"decision-{activation_id}-{gate['id']}.json",
                    # The human must be able to decide from the message alone.
                    "note": "Include what is being authorized, its exact scope, and what denial means.",
                }
                if bound:
                    # Carry the exact pinned subject so the human sees the
                    # precise PR/head being decided.
                    question["subject"] = pinned
                error_key = (activation_id, gate["id"])
                if error_key in decision_errors:
                    question["previous_decision_error"] = decision_errors.pop(error_key)
                print(f"bridge: asking {gate['id']} on {node_id} ({activation_id})", flush=True)
                ask(args.webhook_url, question)

                decision, decision_file, reason = wait_for_decision(
                    ctl, decisions, args.workflow_id, activation_id, gate, gate_timeout, poll
                )
                if decision is None:
                    if reason == "terminal":
                        break  # workflow ended while waiting; stop asking anyone
                    if reason.startswith("invalid decision file"):
                        # Malformed file: surface the parse error on the next
                        # ask instead of re-asking silently.
                        decision_errors[error_key] = reason
                    continue  # timeout or unparked; the next tick re-asks
                try:
                    command = build_gate_command(
                        args.workflow_id, node_id, activation_id, gate, decision,
                        pinned if bound else None,
                    )
                except ValueError as exc:
                    # The human's answer is invalid (bad operation, missing
                    # actor/reason, subject type mismatch). Park the file under
                    # rejected/ so the same answer is not re-read forever, and
                    # carry the reason on the next ask.
                    rejected = move_aside(decisions, activation_id, gate_id, decision_file)
                    decision_errors[error_key] = str(exc)
                    print(f"bridge: rejected decision on {gate['id']}: {exc}", flush=True)
                    continue
                if bound:
                    # A new head creates a new activation with a new pin; the
                    # decision was made against the old subject. Re-inspect and
                    # confirm the activation is still parked on the same pinned
                    # subject before submitting; otherwise drop the decision.
                    ok, why = subject_unchanged(
                        ctl, args.workflow_id, activation_id, gate_id, pinned
                    )
                    if not ok:
                        move_aside(decisions, activation_id, gate_id, decision_file)
                        print(
                            f"bridge: {gate_id} on {activation_id} {why} before "
                            f"submit; dropping decision",
                            flush=True,
                        )
                        continue
                ctl.call(
                    "workflow.command",
                    {"workflow_id": args.workflow_id, "command": command},
                )
                # Archive only once the kernel accepted the decision; a failed
                # record leaves the file in place so the next tick retries it
                # (the kernel's gate idempotency key makes that a safe no-op).
                recorded = decisions / "recorded" / (
                    f"{activation_id}-{gate['id']}-{command['response_hash']}.json"
                )
                recorded.parent.mkdir(parents=True, exist_ok=True)
                decision_file.rename(recorded)
                print(
                    f"bridge: recorded {decision['decision']} by {decision['actor']} "
                    f"on {gate['id']} ({activation_id})", flush=True
                )
        except KeyboardInterrupt:
            print("bridge: interrupted", flush=True)
            return 130
        except Exception as exc:  # transient RPC/webhook failures must not kill the watcher
            print(f"bridge: {type(exc).__name__}: {exc}; backing off {BACKOFF_SECONDS:.0f}s", flush=True)
            traceback.print_exc()
            ctl = None
            time.sleep(BACKOFF_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
