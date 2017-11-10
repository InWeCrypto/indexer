package neo

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dynamicgo/config"
)

var neocnf *config.Config

func init() {
	neocnf, _ = config.NewFromFile("./neo.json")

	logger.DebugF("config :\n%s", neocnf)
}

type testProducer struct {
	indexer *Indexer
}

func (producer *testProducer) Produce(topic string, key []byte, content interface{}) error {
	logger.DebugF("topic %s", topic)
	producer.indexer.Stop()
	return nil
}

func TestIndexer(t *testing.T) {
	producer := &testProducer{}

	indexer, err := NewIndexer(neocnf, producer)

	producer.indexer = indexer

	assert.NoError(t, err)

	indexer.Run()
}
