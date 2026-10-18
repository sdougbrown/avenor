package askstore_test

import (
	"testing"

	"github.com/sdougbrown/avenor/internal/messaging/askstore"
	"github.com/sdougbrown/avenor/internal/messaging/messagetest"
)

func TestStoreContract(t *testing.T) {
	messagetest.RunStoreSuite(t, func(t *testing.T) *askstore.Store {
		store, _ := messagetest.NewMemoryStore(t)
		return store
	})
}
