package phaseconfig

type LoopMarker struct {
	Directive string
	Label     string
}

func LoopDirectiveSeverity(d string) int {
	switch d {
	case "abort":
		return 3
	case "exit":
		return 2
	case "continue":
		return 1
	default:
		return 0
	}
}