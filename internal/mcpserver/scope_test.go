package mcpserver

import (
	"testing"
)

func TestScopeAgentExcludesControlTools(t *testing.T) {
	s, err := NewServer(Options{Transport: "stdio", Scope: ScopeAgent})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	registered := s.RegisteredToolNames()

	// Agent scope lists exactly the read-only introspection tools.
	want := map[string]bool{
		"avenor_status":           true,
		"avenor_result":           true,
		"avenor_events":           true,
		"avenor_workflow_status":  true,
		"avenor_workflow_inspect": true,
		"avenor_workflow_events":  true,
	}
	if len(registered) != len(want) {
		t.Fatalf("agent scope registered %d tools, want %d: %v", len(registered), len(want), registered)
	}
	for _, name := range registered {
		if !want[name] {
			t.Errorf("agent scope registered unexpected tool %q", name)
		}
	}

	// Orchestrate, admin, and control tools must never be listed in agent
	// scope: they are absent at registration time, not hidden by permission.
	excluded := []string{
		"avenor_spawn",
		"avenor_shutdown",
		"avenor_answer_permission",
		"avenor_follow_up",
		"avenor_workflow_wait",
		"avenor_workflow_complete",
		"avenor_workflow_gate",
	}
	for _, name := range excluded {
		for _, got := range registered {
			if got == name {
				t.Errorf("agent scope must not register %q", name)
			}
		}
	}
}

func TestScopeSupervisorRegistersAllTools(t *testing.T) {
	s, err := NewServer(Options{Transport: "stdio", Scope: ScopeSupervisor})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	registered := s.RegisteredToolNames()
	if len(registered) != len(supervisorToolNames) {
		t.Fatalf("supervisor scope registered %d tools, want %d: %v", len(registered), len(supervisorToolNames), registered)
	}
	want := make(map[string]bool, len(supervisorToolNames))
	for _, name := range supervisorToolNames {
		want[name] = true
	}
	for _, name := range registered {
		if !want[name] {
			t.Errorf("supervisor scope registered unexpected tool %q", name)
		}
	}
}

func TestScopeDefaultIsSupervisor(t *testing.T) {
	s, err := NewServer(Options{Transport: "stdio"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	registered := s.RegisteredToolNames()
	if len(registered) != len(supervisorToolNames) {
		t.Fatalf("default scope registered %d tools, want %d (supervisor surface)", len(registered), len(supervisorToolNames))
	}
}

// TestScopeMetadataConsistency guards the scope tables against drift: every
// agent-scope tool must be part of the full supervisor surface, and the full
// surface must be unique and complete. This keeps the filtered registered-tool
// metadata consistent with the registerScopeTool registrations.
func TestScopeMetadataConsistency(t *testing.T) {
	seen := make(map[string]bool, len(supervisorToolNames))
	for _, name := range supervisorToolNames {
		if seen[name] {
			t.Errorf("duplicate tool name in supervisorToolNames: %s", name)
		}
		seen[name] = true
	}
	for name := range agentScopeTools {
		if !seen[name] {
			t.Errorf("agentScopeTools names %q not present in supervisorToolNames", name)
		}
	}
}
