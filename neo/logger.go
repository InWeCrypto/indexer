package neo

import "github.com/dynamicgo/slf4go"

var logger slf4go.Logger

// OpenLogger .
func OpenLogger() {
	logger = slf4go.Get("neo-indexer")
}
