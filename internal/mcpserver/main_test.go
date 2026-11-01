package mcpserver

import (
	"os"
	"testing"

	avruntime "github.com/sdougbrown/avenor/internal/runtime"
	"github.com/sdougbrown/avenor/internal/token"
)

// TestMain keeps an enclosing session's broker out of the package's tests. The
// real supervisors these tests start inherit the environment, and one that
// finds a parent broker's address registers with that broker, so a developer
// running the tests from inside an Avenor session would see the peer listing
// fail. A test that needs a broker sets these itself.
func TestMain(m *testing.M) {
	for _, key := range []string{
		avruntime.EnvBrokerURL,
		avruntime.EnvBrokerRunID,
		avruntime.EnvBrokerToken,
		token.EnvBrokerParentURL,
		token.EnvBrokerParentToken,
		token.EnvBrokerAdvertiseURL,
	} {
		if err := os.Unsetenv(key); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}
