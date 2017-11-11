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
	Monitor *Monitor
}

func (producer *testProducer) Produce(topic string, key []byte, content interface{}) error {
	logger.DebugF("topic %s", topic)
	producer.Monitor.Stop()
	return nil
}

func TestMonitor(t *testing.T) {
	producer := &testProducer{}

	Monitor, err := NewMonitor(neocnf, producer)

	producer.Monitor = Monitor

	assert.NoError(t, err)

	Monitor.Run()
}
