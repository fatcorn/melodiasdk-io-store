package types

import (
	"cosmossdk.io/log"
)

const DefaultCacheSizeLimit = 4000000 // TODO: revert back to 1000000 after paritioning r/w caches

// Context is an interface used by an App to pass context information
// needed to process store streaming requests.
type Context interface {
	BlockHeight() int64
	Logger() log.Logger
	StreamingManager() StreamingManager
}
