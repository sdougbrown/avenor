//go:build linux

package main

import (
	"syscall"
	"testing"
)

// TestConversationFeedKilledOutrightStopsItsSupervisor proves the route a host has
// left when it does not stop the feed gracefully: the kernel tells the supervisor
// its launcher is gone, and the supervisor shuts itself down instead of outliving
// the session. A host that stops its feed with SIGTERM and a kill escalation two
// seconds later reaches this path whenever the graceful stop is still running.
func TestConversationFeedKilledOutrightStopsItsSupervisor(t *testing.T) {
	drive := newConversationDrive(t)
	feed := drive.startFeed(t, "ses_killed")
	socket, _ := drive.awaitConversationSupervisor(t, "ses_killed")

	if err := feed.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("kill feed: %v", err)
	}
	_ = feed.exit(t)
	waitForSupervisorGone(t, socket)
}
