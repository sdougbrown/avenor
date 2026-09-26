"""Tests for the escalation bridge's kernel-facing logic.

Covers the pure helpers that mediate between the transport's decision files
and the kernel's gate command contract: duration parsing, pending-gate
derivation from inspect snapshots, output decoding, decision-to-command
translation, and the decision-file wait's reason paths.

Run: python3 -m unittest discover -s templates/escalation-bridge -v
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import bridge


def parked_activation(act_id="act_1", node_id="merge-auth"):
    return {
        "activation_id": act_id,
        "node_id": node_id,
        "status": "awaiting_gate",
        "selected_outcome": "authorized",
    }


def merge_auth_gate():
    return {
        "id": "merge-authorization",
        "name": "Merge authorization",
        "type": "human",
        "required": True,
        "subject_type": "pull_request",
        "allowed_outcomes": ["authorized"],
    }


class ParseDurationTest(unittest.TestCase):
    def test_units(self):
        self.assertEqual(bridge.parse_duration("30s"), 30)
        self.assertEqual(bridge.parse_duration("5m"), 300)
        self.assertEqual(bridge.parse_duration("1h"), 3600)

    def test_rejects_garbage(self):
        for bad in ("", "30", "30x", "m5", "-5m"):
            with self.assertRaises(ValueError, msg=bad):
                bridge.parse_duration(bad)


class PendingHumanGatesTest(unittest.TestCase):
    def setUp(self):
        self.gates = {"merge-auth": [merge_auth_gate()]}

    def test_yields_parked_required_human_gate(self):
        detail = {"activations": [parked_activation()], "gates": None}
        found = list(bridge.pending_human_gates(detail, self.gates))
        self.assertEqual(len(found), 1)
        act, gate = found[0]
        self.assertEqual(act["activation_id"], "act_1")
        self.assertEqual(gate["id"], "merge-authorization")

    def test_skips_unparked_activations(self):
        act = parked_activation()
        act["status"] = "satisfied"
        detail = {"activations": [act], "gates": None}
        self.assertEqual(list(bridge.pending_human_gates(detail, self.gates)), [])

    def test_skips_settled_gate_instances(self):
        for settled_status in ("passed", "waived"):
            with self.subTest(status=settled_status):
                detail = {
                    "activations": [parked_activation()],
                    "gates": [{"activation_id": "act_1", "gate_id": "merge-authorization",
                               "status": settled_status}],
                }
                self.assertEqual(list(bridge.pending_human_gates(detail, self.gates)), [])

    def test_skips_unrequired_gates(self):
        gate = merge_auth_gate()
        gate["required"] = False
        detail = {"activations": [parked_activation()], "gates": None}
        self.assertEqual(list(bridge.pending_human_gates(detail, {"merge-auth": [gate]})), [])

    def test_tolerates_null_lists(self):
        self.assertEqual(list(bridge.pending_human_gates({}, self.gates)), [])


class LatestOutputsTest(unittest.TestCase):
    def test_decodes_and_keeps_highest_revision(self):
        detail = {"outputs": [
            {"definition_id": "head_sha", "revision": 1, "value": '"old"'},
            {"definition_id": "head_sha", "revision": 2, "value": '"abc123"'},
            {"definition_id": "pull_number", "revision": 1, "value": "123"},
        ]}
        self.assertEqual(
            bridge.latest_outputs(detail),
            {"head_sha": "abc123", "pull_number": 123},
        )

    def test_tolerates_null_and_raw_values(self):
        detail = {"outputs": [{"definition_id": "x", "revision": 1, "value": None}]}
        self.assertEqual(bridge.latest_outputs(detail), {"x": None})


class PinnedSubjectTest(unittest.TestCase):
    def test_returns_pinned_subject_when_resolved(self):
        act = {"activation_id": "act_1",
               "resolved_gates": {"g1": {"subject": {
                   "type": "pull_request", "repository": "org/repo",
                   "pull_request": 123, "revision": "abc123"}}}}
        self.assertEqual(
            bridge.pinned_subject(act, "g1"),
            {"type": "pull_request", "repository": "org/repo",
             "pull_request": 123, "revision": "abc123"},
        )

    def test_returns_none_when_unresolved(self):
        # An unresolved pin has no subject key (omitempty) and lists the
        # unresolved fields instead.
        act = {"activation_id": "act_1",
               "resolved_gates": {"g1": {"unresolved": ["subject.repository"]}}}
        self.assertIsNone(bridge.pinned_subject(act, "g1"))

    def test_returns_none_when_gate_absent(self):
        act = {"activation_id": "act_1",
               "resolved_gates": {"other": {"subject": {}}}}
        self.assertIsNone(bridge.pinned_subject(act, "g1"))

    def test_returns_none_when_no_resolved_gates(self):
        act = {"activation_id": "act_1"}
        self.assertIsNone(bridge.pinned_subject(act, "g1"))


class BuildGateCommandTest(unittest.TestCase):
    def setUp(self):
        self.gate = merge_auth_gate()
        self.decision = {
            "decision": "satisfy",
            "actor": "austin",
            "reason": "Diff reviewed; approved",
            "evidence": "https://slack.com/archives/C123/p1650",
            "subject": {"type": "pull_request", "repository": "org/repo",
                        "pull_request": 123, "revision": "abc123"},
        }

    def test_valid_satisfy(self):
        command = bridge.build_gate_command("wf_1", "merge-auth", "act_1", self.gate, self.decision)
        self.assertEqual(command["operation"], "satisfy")
        self.assertEqual(command["actor"], "austin")
        self.assertEqual(command["node_id"], "merge-auth")
        self.assertEqual(command["activation_id"], "act_1")
        self.assertEqual(command["gate_id"], "merge-authorization")
        self.assertEqual(command["subject"]["revision"], "abc123")
        self.assertEqual(command["source"], "escalation-bridge")
        # Kernel requires at least one evidence id; the transport pointer
        # travels in the reason so it survives in the gate history.
        self.assertEqual(len(command["evidence_ids"]), 1)
        self.assertTrue(command["evidence_ids"][0].startswith("ev_bridge_"))
        self.assertIn("slack.com/archives", command["reason"])

    def test_response_hash_is_stable_per_decision(self):
        first = bridge.build_gate_command("wf_1", "n", "act_1", self.gate, self.decision)
        second = bridge.build_gate_command("wf_1", "n", "act_1", self.gate, dict(self.decision))
        self.assertEqual(first["response_hash"], second["response_hash"])
        # The hash doubles as the evidence id and archive filename, so it must
        # be a fixed width and must distinguish decisions.
        self.assertEqual(len(first["response_hash"]), 32)
        other = dict(self.decision, reason="declined after review")
        third = bridge.build_gate_command("wf_1", "n", "act_1", self.gate, other)
        self.assertNotEqual(first["response_hash"], third["response_hash"])

    def test_reject_requires_operation_value(self):
        self.decision["decision"] = "approve"
        with self.assertRaises(ValueError):
            bridge.build_gate_command("wf_1", "n", "act_1", self.gate, self.decision)

    def test_rejects_missing_actor_or_reason(self):
        for field in ("actor", "reason"):
            broken = {k: v for k, v in self.decision.items() if k != field}
            with self.assertRaises(ValueError, msg=field):
                bridge.build_gate_command("wf_1", "n", "act_1", self.gate, broken)

    def test_subject_required_when_gate_declares_subject_type(self):
        broken = {k: v for k, v in self.decision.items() if k != "subject"}
        with self.assertRaises(ValueError):
            bridge.build_gate_command("wf_1", "n", "act_1", self.gate, broken)

    def test_subject_type_must_match_declaration(self):
        self.decision["subject"]["type"] = "commit"
        with self.assertRaises(ValueError):
            bridge.build_gate_command("wf_1", "n", "act_1", self.gate, self.decision)

    def test_subject_type_enforced_only_when_declared(self):
        gate = merge_auth_gate()
        del gate["subject_type"]
        command = bridge.build_gate_command("wf_1", "n", "act_1", gate, {
            "decision": "satisfy", "actor": "a", "reason": "r",
        })
        self.assertNotIn("subject", command)

    def test_outcome_passthrough(self):
        self.decision["outcome"] = "authorized"
        command = bridge.build_gate_command("wf_1", "n", "act_1", self.gate, self.decision)
        self.assertEqual(command["outcome"], "authorized")

    def test_bound_gate_submits_pinned_subject_not_transport(self):
        # The transport supplies a different subject; a bound gate must submit
        # exactly the pinned one, ignoring the transport's.
        self.decision["subject"] = {"type": "pull_request", "repository": "wrong/repo",
                                    "pull_request": 999, "revision": "deadbeef"}
        pinned = {"type": "pull_request", "repository": "org/repo",
                  "pull_request": 123, "revision": "abc123"}
        command = bridge.build_gate_command("wf_1", "n", "act_1", self.gate,
                                           self.decision, pinned)
        self.assertEqual(command["subject"], pinned)

    def test_bound_gate_requires_no_transport_subject(self):
        # A bound gate submits the pinned subject even when the transport
        # supplies none at all.
        broken = {k: v for k, v in self.decision.items() if k != "subject"}
        pinned = {"type": "pull_request", "repository": "org/repo",
                  "pull_request": 123, "revision": "abc123"}
        command = bridge.build_gate_command("wf_1", "n", "act_1", self.gate,
                                           broken, pinned)
        self.assertEqual(command["subject"], pinned)


class FakeControl:
    """Minimal stand-in for bridge.Control returning canned inspect results."""

    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def call(self, method, params=None, timeout=35.0):
        self.calls.append(method)
        return self.results.pop(0)


class WaitForDecisionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.decisions = Path(self.tmp.name)
        self.gate = merge_auth_gate()

    def write_decision(self, payload):
        path = self.decisions / "decision-act_1-merge-authorization.json"
        path.write_text(payload if isinstance(payload, str) else json.dumps(payload))
        return path

    def test_answered(self):
        self.write_decision({"decision": "satisfy", "actor": "a", "reason": "r"})
        ctl = FakeControl([])
        decision, path, reason = bridge.wait_for_decision(
            ctl, self.decisions, "wf_1", "act_1", self.gate, 5, 0.01
        )
        self.assertEqual(reason, "answered")
        self.assertEqual(decision["decision"], "satisfy")
        self.assertTrue(path.exists())

    def test_answered_after_file_arrives_on_a_later_poll(self):
        # The real-world sequence: the transport writes the decision file only
        # after the bridge asks, so the file must be re-checked each poll tick.
        detail = {"activations": [parked_activation()], "gates": None,
                  "instance": {"status": "active"}}

        class LateTransport(FakeControl):
            def call(self, method, params=None, timeout=35.0):
                result = super().call(method, params, timeout)
                self.decisions.joinpath(
                    "decision-act_1-merge-authorization.json"
                ).write_text(json.dumps({"decision": "satisfy", "actor": "a", "reason": "r"}))
                return result

        ctl = LateTransport([detail])
        ctl.decisions = self.decisions
        decision, _, reason = bridge.wait_for_decision(
            ctl, self.decisions, "wf_1", "act_1", self.gate, 5, 0.01
        )
        self.assertEqual(reason, "answered")
        self.assertEqual(decision["decision"], "satisfy")

    def test_terminal_reason(self):
        detail = {"activations": [parked_activation()], "gates": None,
                  "instance": {"status": "completed"}}
        ctl = FakeControl([detail])
        _, _, reason = bridge.wait_for_decision(
            ctl, self.decisions, "wf_1", "act_1", self.gate, 5, 0.01
        )
        self.assertEqual(reason, "terminal")

    def test_unparked_reason(self):
        detail = {"activations": [{**parked_activation(), "status": "satisfied"}],
                  "gates": None, "instance": {"status": "active"}}
        ctl = FakeControl([detail])
        _, _, reason = bridge.wait_for_decision(
            ctl, self.decisions, "wf_1", "act_1", self.gate, 5, 0.01
        )
        self.assertEqual(reason, "unparked")

    def test_timeout_reason(self):
        detail = {"activations": [parked_activation()], "gates": None,
                  "instance": {"status": "active"}}
        ctl = FakeControl([detail])
        # Script the monotonic clock but fall back to a value past the
        # deadline for any extra call, so a loop change fails the assertion
        # instead of raising StopIteration inside the patched clock.
        scripted = iter([0, 0, 100])
        with mock.patch.object(bridge.time, "monotonic",
                               side_effect=lambda: next(scripted, 100)):
            _, _, reason = bridge.wait_for_decision(
                ctl, self.decisions, "wf_1", "act_1", self.gate, 5, 0.01
            )
        self.assertEqual(reason, "timeout")

    def test_malformed_file_moves_aside_and_reports(self):
        self.write_decision("{not json")
        ctl = FakeControl([])
        decision, _, reason = bridge.wait_for_decision(
            ctl, self.decisions, "wf_1", "act_1", self.gate, 5, 0.01
        )
        self.assertIsNone(decision)
        self.assertIn("invalid decision file", reason)
        rejected = list((self.decisions / "rejected").glob("*.json"))
        self.assertEqual(len(rejected), 1)


class SubjectUnchangedTest(unittest.TestCase):
    """The pre-submit re-inspect of a bound-gate decision."""

    PINNED = {"type": "pull_request", "repository": "org/repo",
              "pull_request": 123, "revision": "abc123"}

    def inspect(self, act):
        return {"activations": [act], "gates": None,
                "instance": {"status": "active"}}

    def test_ok_when_still_parked_with_same_pinned_subject(self):
        act = parked_activation()
        act["resolved_gates"] = {"g1": {"subject": dict(self.PINNED)}}
        ctl = FakeControl([self.inspect(act)])
        ok, why = bridge.subject_unchanged(ctl, "wf_1", "act_1", "g1", self.PINNED)
        self.assertTrue(ok)
        self.assertEqual(why, "ok")

    def test_unparked_when_activation_resolved(self):
        act = parked_activation()
        act["status"] = "satisfied"
        act["resolved_gates"] = {"g1": {"subject": dict(self.PINNED)}}
        ctl = FakeControl([self.inspect(act)])
        ok, why = bridge.subject_unchanged(ctl, "wf_1", "act_1", "g1", self.PINNED)
        self.assertFalse(ok)
        self.assertEqual(why, "unparked")

    def test_subject_changed_when_pinned_subject_differs(self):
        act = parked_activation()
        changed = dict(self.PINNED, revision="deadbeef")
        act["resolved_gates"] = {"g1": {"subject": changed}}
        ctl = FakeControl([self.inspect(act)])
        ok, why = bridge.subject_unchanged(ctl, "wf_1", "act_1", "g1", self.PINNED)
        self.assertFalse(ok)
        self.assertEqual(why, "subject changed")


class BoundGateLoopTest(unittest.TestCase):
    """Loop-level tests: main() driven over a scripted control socket.

    Each run scripts the sequence of workflow.wait / workflow.inspect results
    the bridge consumes, stubs the webhook and time.sleep so the loop runs
    fast, and asserts on what the bridge asked and submitted.
    """

    PINNED = {"type": "pull_request", "repository": "org/repo",
              "pull_request": 123, "revision": "abc123"}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.decisions = Path(self.tmp.name)
        self.template = self.decisions / "template.json"
        self.template.write_text(json.dumps({"nodes": [
            {"id": "merge-auth", "gates": [{
                "id": "merge-authorization", "type": "human", "required": True,
                "subject_binding": {"type": "pull_request"},
            }]},
        ]}))
        self.asked = []

    def run_bridge(self, results):
        ctl = FakeControl(results)
        argv = ["bridge.py", "--socket", "/tmp/unused.sock", "--workflow-id", "wf_1",
                "--webhook-url", "http://127.0.0.1:1/ask",
                "--decision-dir", str(self.decisions),
                "--template", str(self.template),
                "--wait", "1s", "--gate-timeout", "5m", "--poll", "1s"]
        with mock.patch.object(bridge, "Control", lambda path: ctl), \
                mock.patch.object(bridge, "ask", lambda url, q: self.asked.append(q)), \
                mock.patch.object(bridge.time, "sleep", lambda *_: None), \
                mock.patch("sys.argv", argv):
            rc = bridge.main()
        return rc, ctl

    def inspect(self, subject):
        act = parked_activation()
        resolved = {"unresolved": ["subject.repository"]} if subject is None else {"subject": subject}
        act["resolved_gates"] = {"merge-authorization": resolved}
        return {"activations": [act], "gates": None, "instance": {"status": "active"}}

    def test_unresolved_pin_skips_ask_until_resolved(self):
        # Tick 1: the bound gate's pin is unresolved -> no ask. Tick 2: the
        # pin has resolved -> the question carries the exact pinned subject.
        results = [
            {"terminal": False},                       # wait 1
            self.inspect(None),                        # inspect 1: unresolved
            {"terminal": False},                       # wait 2
            self.inspect(self.PINNED),                 # inspect 2: resolved
            {**self.inspect(self.PINNED),              # wait_for_decision re-check
             "activations": [{**parked_activation(), "status": "satisfied"}]},
            {"terminal": True, "instance": {"status": "completed", "terminal_outcome": "done"}},
        ]
        rc, ctl = self.run_bridge(results)
        self.assertEqual(rc, 0)
        self.assertEqual(len(self.asked), 1)
        question = self.asked[0]
        self.assertEqual(question["gate_id"], "merge-authorization")
        self.assertEqual(question["activation_id"], "act_1")
        self.assertEqual(question["subject"], self.PINNED)
        self.assertNotIn("workflow.command", ctl.calls)

    def test_stale_subject_before_submit_rejects_decision(self):
        # The decision file arrives, but the pre-submit re-inspect shows a
        # different pinned subject (a new head): no command is submitted and
        # the decision file is moved into rejected/.
        changed = dict(self.PINNED, revision="deadbeef")
        (self.decisions / "decision-act_1-merge-authorization.json").write_text(
            json.dumps({"decision": "satisfy", "actor": "austin", "reason": "looks good"}))
        results = [
            {"terminal": False},        # wait 1
            self.inspect(self.PINNED),  # inspect 1: ask + read decision file
            self.inspect(changed),      # subject_unchanged re-inspect: changed
            {"terminal": True, "instance": {"status": "completed", "terminal_outcome": "done"}},
        ]
        rc, ctl = self.run_bridge(results)
        self.assertEqual(rc, 0)
        self.assertEqual(len(self.asked), 1)
        self.assertEqual(self.asked[0]["subject"], self.PINNED)
        self.assertNotIn("workflow.command", ctl.calls)
        rejected = list((self.decisions / "rejected").glob("act_1-*.json"))
        self.assertEqual(len(rejected), 1)
        self.assertFalse((self.decisions / "decision-act_1-merge-authorization.json").exists())


class LoadHumanGatesTest(unittest.TestCase):
    def test_maps_only_human_gates_by_node(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "t.json"
            path.write_text(json.dumps({"nodes": [
                {"id": "a", "gates": [{"id": "g1", "type": "human"}]},
                {"id": "b", "gates": [{"id": "g2", "type": "external"}]},
                {"id": "c"},
            ]}))
            gates = bridge.load_human_gates(path)
        self.assertEqual(list(gates), ["a"])
        self.assertEqual(gates["a"][0]["id"], "g1")


if __name__ == "__main__":
    unittest.main()
