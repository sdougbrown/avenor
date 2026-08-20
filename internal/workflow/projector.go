package workflow

// projector.go renders deterministic, derived Markdown projections for a
// workflow instance. Projections are derived artifacts — never authoritative
// and never editable by agents. Given the same Snapshot they always produce
// byte-identical output: they emit no wall-clock values, iterate slices in
// stored order, and never range over maps.

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// mdEsc escapes a field value for safe use in generated Markdown: pipe char
// and all control characters (newline, CR, tab, and 0x00-0x1F) are replaced.
func mdEsc(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == '|':
			b.WriteString(`\|`)
		case r < 0x20 || r == 0x7f || r == '\n' || r == '\r' || r == '\t':
			b.WriteRune(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// WriteProjections renders every projection for snap and writes the files
// under dir (the instance directory). It creates node directories as needed.
// A projection is a derived artifact: callers (the store) treat an error as
// non-fatal, so this never blocks a committed state transition.
func WriteProjections(dir string, snap Snapshot) error {
	if err := os.WriteFile(filepath.Join(dir, "workflow.md"), []byte(ProjectWorkflowMD(snap)), 0o644); err != nil {
		return err
	}
	for _, nodeID := range distinctNodes(snap.Instance.Activations) {
		if !safeComponent(string(nodeID)) {
			continue // never write outside the instance dir (path traversal guard)
		}
		nodeDir := filepath.Join(dir, "nodes", string(nodeID))
		if err := os.MkdirAll(nodeDir, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(nodeDir, "execution.md"), []byte(ProjectExecutionMD(snap, nodeID)), 0o644); err != nil {
			return err
		}
		for i, gi := range gatesForNode(snap, nodeID) {
			name := fmt.Sprintf("review-%d.md", i+1)
			if err := os.WriteFile(filepath.Join(nodeDir, name), []byte(ProjectReviewMD(snap, gi)), 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

// ProjectWorkflowMD renders the instance-level workflow.md.
func ProjectWorkflowMD(snap Snapshot) string {
	var b strings.Builder
	inst := snap.Instance
	b.WriteString("# Workflow " + mdEsc(string(inst.WorkflowID)) + "\n\n")
	b.WriteString("- Template: " + mdEsc(string(inst.TemplateID)) + "@" + mdEsc(string(inst.TemplateVersion)) + "\n")
	b.WriteString("- Instance: " + mdEsc(string(inst.InstanceID)) + "\n")
	b.WriteString("- Status: " + mdEsc(string(inst.Status)) + "\n")
	b.WriteString("- Revision: " + strconv.FormatInt(inst.Revision, 10) + "\n")
	if inst.TerminalOutcome != "" {
		b.WriteString("- Terminal outcome: " + mdEsc(string(inst.TerminalOutcome)) + "\n")
	}
	b.WriteString("\n")

	b.WriteString("## Nodes\n\n")
	writeTable(&b, []string{"Node", "Activations"}, nodeRows(snap.Instance.Activations))

	b.WriteString("\n## Activations\n\n")
	writeTable(&b, []string{"Activation", "Node", "Iteration", "Status", "Outcome"}, activationRows(snap.Instance.Activations))

	b.WriteString("\n## Gates\n\n")
	writeTable(&b, []string{"Gate Instance", "Node", "Activation", "Status", "Outcome", "Actor"}, gateRows(snap))

	b.WriteString("\n## Attempts\n\n")
	writeTable(&b, []string{"Attempt", "Node", "Activation", "Status", "Backend", "Model"}, attemptRows(snap.Instance.Attempts))

	return b.String()
}

// ProjectExecutionMD renders a node's execution.md: that node's activations,
// attempts, evidence, and gate instances.
func ProjectExecutionMD(snap Snapshot, nodeID NodeID) string {
	var b strings.Builder
	acts := activationsForNode(snap.Instance.Activations, nodeID)
	activationIDs := activationIDSet(acts)

	b.WriteString("# Node " + mdEsc(string(nodeID)) + "\n\n")

	b.WriteString("## Activations\n\n")
	writeTable(&b, []string{"Activation", "Iteration", "Status", "Outcome"}, activationRows(acts))

	b.WriteString("\n## Attempts\n\n")
	rows := make([][]string, 0, len(snap.Instance.Attempts))
	for _, a := range snap.Instance.Attempts {
		if a.Identity.NodeID == nodeID {
			rows = append(rows, []string{string(a.ID), string(a.Identity.ActivationID), string(a.Status), a.Backend, a.Model})
		}
	}
	writeTable(&b, []string{"Attempt", "Activation", "Status", "Backend", "Model"}, rows)

	b.WriteString("\n## Evidence\n\n")
	evRows := make([][]string, 0, len(snap.Instance.Evidence))
	for _, e := range snap.Instance.Evidence {
		if activationIDs[e.ActivationID] {
			rows := []string{string(e.ID), e.Kind, string(e.Source), e.Authority}
			if e.StoredPath != "" {
				rows = append(rows, e.StoredPath)
			} else {
				rows = append(rows, "")
			}
			if e.SHA256 != "" {
				rows = append(rows, e.SHA256)
			} else {
				rows = append(rows, "")
			}
			evRows = append(evRows, rows)
		}
	}
	writeTable(&b, []string{"Evidence", "Kind", "Source", "Authority", "Stored Path", "SHA256"}, evRows)

	b.WriteString("\n## Gates\n\n")
	rows2 := make([][]string, 0, len(snap.Instance.Gates))
	for _, gi := range snap.Instance.Gates {
		if activationIDs[gi.ActivationID] {
			rows2 = append(rows2, []string{string(gi.ID), string(gi.ActivationID), string(gi.Status), string(gi.Outcome), gi.Actor})
		}
	}
	writeTable(&b, []string{"Gate Instance", "Activation", "Status", "Outcome", "Actor"}, rows2)

	return b.String()
}

// ProjectReviewMD renders a gate instance's review-N.md: the gate summary plus
// the evidence it references.
func ProjectReviewMD(snap Snapshot, gi GateInstance) string {
	var b strings.Builder
	nodeID := activationNode(snap.Instance.Activations, gi.ActivationID)

	b.WriteString("# Gate " + mdEsc(string(gi.ID)) + "\n\n")
	b.WriteString("- Gate: " + mdEsc(string(gi.GateID)) + "\n")
	b.WriteString("- Node: " + mdEsc(string(nodeID)) + "\n")
	b.WriteString("- Activation: " + mdEsc(string(gi.ActivationID)) + "\n")
	b.WriteString("- Status: " + mdEsc(string(gi.Status)) + "\n")
	if gi.Outcome != "" {
		b.WriteString("- Outcome: " + mdEsc(string(gi.Outcome)) + "\n")
	}
	if gi.Actor != "" {
		b.WriteString("- Actor: " + mdEsc(gi.Actor) + "\n")
	}
	if gi.Reason != "" {
		b.WriteString("- Reason: " + mdEsc(gi.Reason) + "\n")
	}
	if gi.Source != "" {
		b.WriteString("- Source: " + mdEsc(gi.Source) + "\n")
	}
	b.WriteString("\n")

	b.WriteString("## Evidence\n\n")
	evidenceByID := make(map[EvidenceID]Evidence, len(snap.Instance.Evidence))
	for _, e := range snap.Instance.Evidence {
		evidenceByID[e.ID] = e
	}
	rows := make([][]string, 0, len(gi.EvidenceIDs))
	for _, id := range gi.EvidenceIDs {
		e, ok := evidenceByID[id]
		if !ok {
			rows = append(rows, []string{mdEsc(string(id)), "", "", ""})
			continue
		}
		rows = append(rows, []string{mdEsc(string(e.ID)), mdEsc(e.Kind), mdEsc(e.Authority), mdEsc(e.SHA256)})
	}
	writeTable(&b, []string{"Evidence", "Kind", "Authority", "SHA256"}, rows)

	return b.String()
}

// --- table rendering ---

// writeTable emits a Markdown table. An empty body emits a single "- none"
// line so the section is explicit and deterministic.
func writeTable(b *strings.Builder, header []string, rows [][]string) {
	if len(rows) == 0 {
		b.WriteString("- none\n")
		return
	}
	b.WriteString("| " + strings.Join(header, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat(" --- |", len(header)) + "\n")
	for _, r := range rows {
		cells := make([]string, 0, len(header))
		for _, c := range r {
			cells = append(cells, mdEsc(c))
		}
		b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
	}
}

// --- row builders (all iterate in stored order) ---

func activationRows(acts []Activation) [][]string {
	rows := make([][]string, 0, len(acts))
	for _, a := range acts {
		rows = append(rows, []string{string(a.ID), string(a.NodeID), strconv.Itoa(a.Iteration), string(a.Status), string(a.SelectedOutcome)})
	}
	return rows
}

func gateRows(snap Snapshot) [][]string {
	rows := make([][]string, 0, len(snap.Instance.Gates))
	for _, gi := range snap.Instance.Gates {
		rows = append(rows, []string{string(gi.ID), string(activationNode(snap.Instance.Activations, gi.ActivationID)), string(gi.ActivationID), string(gi.Status), string(gi.Outcome), gi.Actor})
	}
	return rows
}

func attemptRows(attempts []Attempt) [][]string {
	rows := make([][]string, 0, len(attempts))
	for _, a := range attempts {
		rows = append(rows, []string{string(a.ID), string(a.Identity.NodeID), string(a.Identity.ActivationID), string(a.Status), a.Backend, a.Model})
	}
	return rows
}

func nodeRows(acts []Activation) [][]string {
	distinct := distinctNodes(acts)
	counts := make(map[NodeID]int, len(distinct))
	for _, a := range acts {
		counts[a.NodeID]++
	}
	rows := make([][]string, 0, len(distinct))
	for _, n := range distinct {
		rows = append(rows, []string{string(n), strconv.Itoa(counts[n])})
	}
	return rows
}

// --- lookup helpers ---

// distinctNodes returns node IDs in first-seen order across the activations.
func distinctNodes(acts []Activation) []NodeID {
	seen := make(map[NodeID]bool, len(acts))
	out := make([]NodeID, 0, len(acts))
	for _, a := range acts {
		if !seen[a.NodeID] {
			seen[a.NodeID] = true
			out = append(out, a.NodeID)
		}
	}
	return out
}

func activationsForNode(acts []Activation, nodeID NodeID) []Activation {
	out := make([]Activation, 0, len(acts))
	for _, a := range acts {
		if a.NodeID == nodeID {
			out = append(out, a)
		}
	}
	return out
}

// gatesForNode returns the gate instances for a node's activations in stored
// (gate-slice) order.
func gatesForNode(snap Snapshot, nodeID NodeID) []GateInstance {
	ids := activationIDSet(activationsForNode(snap.Instance.Activations, nodeID))
	out := make([]GateInstance, 0, len(snap.Instance.Gates))
	for _, gi := range snap.Instance.Gates {
		if ids[gi.ActivationID] {
			out = append(out, gi)
		}
	}
	return out
}

func activationIDSet(acts []Activation) map[ActivationID]bool {
	out := make(map[ActivationID]bool, len(acts))
	for _, a := range acts {
		out[a.ID] = true
	}
	return out
}

func activationNode(acts []Activation, id ActivationID) NodeID {
	for _, a := range acts {
		if a.ID == id {
			return a.NodeID
		}
	}
	return ""
}
