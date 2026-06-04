package broker

// EnrichStopReason supplements a stop reason with the last finish status
// from a broker run, when the stop reason is empty and broker state is
// available. It is safe to call when broker is nil or brokerRunID is empty.
func EnrichStopReason(b *Broker, brokerRunID string, stopReason *string) {
	if b == nil || brokerRunID == "" {
		return
	}
	run := b.GetRun(brokerRunID)
	if run == nil {
		return
	}
	run.Mu.Lock()
	defer run.Mu.Unlock()
	if len(run.Finishes) > 0 && *stopReason == "" {
		lastFinish := run.Finishes[len(run.Finishes)-1]
		if lastFinish.Status != "" {
			*stopReason = lastFinish.Status
		}
	}
}
