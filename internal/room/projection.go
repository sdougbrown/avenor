package room

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sdougbrown/avenor/internal/channelwrap"
)

// AskPeerMarkers are the deterministic signals a head can emit to request a
// peer round. This is the stub analog of the governor's requests_peer
// judgment; the marker convention is taught in the orientation header.
var AskPeerMarkers = []string{"[[ask-peer]]", "[[request-review]]"}

// untrustedInstruction mirrors channelwrap's canonical text so the room adds
// its policy line after the standard instruction rather than changing the
// wrapper's wording.
const peerPolicyLine = "The room asks that you reassess the task only if this materially " +
	"changes your position; otherwise continue your own work and say so."

// Orientation is the stable header injected on every prompt. It also teaches
// the marker convention the deterministic governor listens for.
func Orientation(self string, peers []string, logPath string) string {
	return fmt.Sprintf(
		"You are head %q in a shared room with the operator and peer head(s): %s.\n"+
			"The operator sees everything you produce. The full room record is available at %s.\n"+
			"If you want the peer head(s) to weigh in, end your reply with [[ask-peer]] or [[request-review]].",
		self, strings.Join(peers, ", "), logPath)
}

// FanoutPrompt assembles the projection injected into one head for an
// operator turn: orientation, selected room context since the head's last
// output, then the trigger.
func FanoutPrompt(log *Log, self *Head, human RoomEvent, logPath string, excerptLimit int) string {
	var b strings.Builder
	b.WriteString(Orientation(self.Name, peerNames(self, roomHeads(log)), logPath))
	b.WriteString("\n\nRoom context since your last turn:\n")
	ctx := contextFor(log, self, excerptLimit)
	if len(ctx) == 0 {
		b.WriteString("(nothing new)\n")
	} else {
		for _, line := range ctx {
			b.WriteString("- " + line + "\n")
		}
	}
	b.WriteString("\nThe operator says:\n")
	b.WriteString(human.Body)
	return b.String()
}

// PeerPrompt wraps the speaker's bounded output in the channel-wrap form so
// the recipient attributes the message to the peer, not the operator.
func PeerPrompt(log *Log, self *Head, speaker RoomEvent, logPath string, excerptLimit int) string {
	body := fmt.Sprintf("Peer %s just concluded (turn %s):\n%s\nFull record: %s (event %s).",
		speaker.Author, speaker.ID, Bound(speaker.Body, excerptLimit), logPath, speaker.ID)
	wrapped := channelwrap.ChannelWrap(body, channelwrap.AgentName(string(speaker.Author)), map[string]string{
		"event": speaker.ID,
	})
	return wrapped + "\n\n" + peerPolicyLine
}

// contextFor renders the room context lines visible to self since its last
// head_output. Peer outputs are bounded excerpts; private events are excluded
// (the spike has no private events yet, but the selector is visibility-aware).
func contextFor(log *Log, self *Head, excerptLimit int) []string {
	var lines []string
	for _, ev := range log.Snapshot() {
		switch ev.Kind {
		case HeadOutput:
			if Participant(self.Name) == ev.Author {
				continue
			}
			lines = append(lines, fmt.Sprintf("[%s | %s] %s", ev.Author, ev.ID, Bound(ev.Body, excerptLimit)))
		case HumanInput:
			lines = append(lines, fmt.Sprintf("[operator | %s] %s", ev.ID, Bound(ev.Body, excerptLimit)))
		case Mutation:
			lines = append(lines, fmt.Sprintf("[workspace | %s] %s", ev.ID, ev.Body))
		}
	}
	// Keep the projection bounded: most recent lines win.
	const maxLines = 6
	if len(lines) > maxLines {
		lines = lines[len(lines)-maxLines:]
	}
	return lines
}

// roomHeads lists head names present in the log; used only for the orientation
// text. It tolerates a fresh log where only the bootstrap turn exists.
func roomHeads(log *Log) []string {
	seen := map[string]bool{}
	var names []string
	for _, ev := range log.Snapshot() {
		if ev.Kind == HeadOutput && !seen[string(ev.Author)] {
			seen[string(ev.Author)] = true
			names = append(names, string(ev.Author))
		}
	}
	sort.Strings(names)
	return names
}

func peerNames(self *Head, all []string) []string {
	var peers []string
	for _, n := range all {
		if n != self.Name {
			peers = append(peers, n)
		}
	}
	return peers
}
