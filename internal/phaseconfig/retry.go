package phaseconfig

import "time"

func BackoffDuration(retry int) time.Duration {
	d := time.Duration(1<<uint(retry+1)) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
