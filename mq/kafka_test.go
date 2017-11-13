package mq

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dynamicgo/config"
)

var cnf *config.Config

func init() {
	var err error

	cnf, err = config.NewFromFile("testdata/config.json")

	if err != nil {
		panic(err)
	}
}

func TestKafkaProducer(t *testing.T) {
	producer, err := NewAliyunProducer(cnf)

	assert.NoError(t, err)

	err = producer.Produce(cnf.GetString("aliyun.kafka.topic", "xxx"), []byte("1"), "test")

	assert.NoError(t, err)

	consumer, err := NewAliyunConsumer(cnf)

	assert.NoError(t, err)

	message, ok := <-consumer.Messages()

	assert.True(t, ok)

	fmt.Println(string(message.Value()))
}
