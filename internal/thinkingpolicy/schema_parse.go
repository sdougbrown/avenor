package thinkingpolicy

import (
	"encoding/json"
	"os"
)

// readSchemaRules reads the Umpire schema JSON and returns the raw rules
// array for introspection. The schema is the single source of truth for
// backend policies, so we parse it at init time to derive the set of
// supported backends rather than maintaining a hand-written list.
func readSchemaRules() ([]map[string]json.RawMessage, error) {
	data, err := os.ReadFile("../../schemas/thinking_policy.umpire.json")
	if err != nil {
		return nil, err
	}
	var schema struct {
		Rules []map[string]json.RawMessage `json:"rules"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}
	return schema.Rules, nil
}

// extractBackendsFromRules scans the eitherOf branch expressions for condIn
// conditions on the "backend" condition, collecting all backend identifiers
// that have thinking support according to the schema.
func extractBackendsFromRules(rules []map[string]json.RawMessage) map[string]bool {
	backends := make(map[string]bool)
	for _, rule := range rules {
		if rule["type"] == nil {
			continue
		}
		var rType string
		if err := json.Unmarshal(rule["type"], &rType); err != nil {
			continue
		}
		if rType != "eitherOf" {
			continue
		}
		var branches map[string]json.RawMessage
		if err := json.Unmarshal(rule["branches"], &branches); err != nil {
			continue
		}
		for _, branchRaw := range branches {
			var branchRules []map[string]json.RawMessage
			if err := json.Unmarshal(branchRaw, &branchRules); err != nil {
				continue
			}
			for _, br := range branchRules {
				collectBackendsFromExpr(br["when"], backends)
			}
		}
	}
	return backends
}

// collectBackendsFromExpr recursively walks an expression AST and collects
// backend names from condIn conditions on the "backend" condition.
func collectBackendsFromExpr(raw json.RawMessage, backends map[string]bool) {
	if raw == nil || len(raw) == 0 {
		return
	}
	var expr map[string]json.RawMessage
	if err := json.Unmarshal(raw, &expr); err != nil {
		return
	}
	var op string
	if err := json.Unmarshal(expr["op"], &op); err != nil {
		return
	}
	if op == "condIn" {
		var cond string
		if err := json.Unmarshal(expr["condition"], &cond); err == nil && cond == "backend" {
			var values []string
			if err := json.Unmarshal(expr["values"], &values); err == nil {
				for _, v := range values {
					backends[v] = true
				}
			}
		}
		return
	}
	// Recurse into compound expressions
	if expr["exprs"] != nil {
		var exprs []json.RawMessage
		if err := json.Unmarshal(expr["exprs"], &exprs); err == nil {
			for _, e := range exprs {
				collectBackendsFromExpr(e, backends)
			}
		}
	}
	if expr["expr"] != nil {
		collectBackendsFromExpr(expr["expr"], backends)
	}
}