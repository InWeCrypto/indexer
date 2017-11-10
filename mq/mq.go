package mq

// Producer mq producer
type Producer interface {
	Produce(topic string, key []byte, content interface{}) error
}
