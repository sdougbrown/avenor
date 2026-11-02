package admission

// ActionApplicability is the per-action input contract: the one table both the
// Go and the TypeScript router validate against. The JSON Schema variants are
// proven equal to it by each tool's schema/table comparison test.
type ActionApplicability struct {
	Access      string                         // read (credential-free) or empty (socket owner)
	Permitted   []string                       // every property the action accepts, including "action"
	Required    []string                       // the subset that must be present
	Types       map[string]string              // declared wire type per permitted property: string, integer, object, array or json
	Enums       map[string][]string            // declared closed value set per property
	Constraints map[string]PropertyConstraints // declared limits per property
}

// PropertyConstraints is one property's declared limits, compiled from the
// published JSON Schema so the router and the schema enforce the same rule from
// one declaration. A zero length means no length limit; HasMinimum and
// HasMaximum keep a declared bound of zero distinct from an absent one.
type PropertyConstraints struct {
	MinLength  int
	MaxLength  int
	Pattern    string
	Minimum    int64  // inclusive lower bound for an integer property
	HasMinimum bool   // keeps a declared bound of zero distinct from an absent one
	Maximum    int64  // inclusive upper bound for an integer property
	HasMaximum bool   // keeps a declared bound of zero distinct from an absent one
	ItemType   string // declared wire type of an array property's items
}
