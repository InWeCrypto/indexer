package mq

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/Shopify/sarama"
	"github.com/dynamicgo/config"
)

// AliyunProducer aliyun mq client using kafka protocol
type AliyunProducer struct {
	producer sarama.SyncProducer
}

// NewAliyunProducer create new aliyun mq client
func NewAliyunProducer(cnf *config.Config) (*AliyunProducer, error) {

	logger.DebugF("create aliyun kafka producer with config:\n%s", cnf)

	kafkaConfig := sarama.NewConfig()

	kafkaConfig.Net.SASL.Enable = true

	kafkaConfig.Net.SASL.User = cnf.GetString("aliyun.kafka.user", "xxxx")
	// The aliyun kafka use SecretKey last 10 chars as password
	kafkaConfig.Net.SASL.Password = cnf.GetString("aliyun.kafka.password", "xxxx")

	kafkaConfig.Net.SASL.Handshake = true

	certBytes, err := ioutil.ReadFile(cnf.GetString("aliyun.kafka.cert", "/etc/inwecrypto/kafka.cert"))

	if err != nil {
		return nil, err
	}

	clientCertPool := x509.NewCertPool()
	ok := clientCertPool.AppendCertsFromPEM(certBytes)
	if !ok {
		return nil, fmt.Errorf("kafka producer failed to parse root certificate")
	}

	kafkaConfig.Net.TLS.Config = &tls.Config{
		//Certificates:       []tls.Certificate{},
		RootCAs:            clientCertPool,
		InsecureSkipVerify: true,
	}

	kafkaConfig.Net.TLS.Enable = true
	kafkaConfig.Producer.Return.Successes = true

	if err = kafkaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("kafka producer config invalidate. err: %v", err)
	}

	var servers = []string{
		"xxxxx",
	}

	cnf.GetObject("aliyun.kafka.servers", &servers)

	producer, err := sarama.NewSyncProducer(servers, kafkaConfig)

	if err != nil {
		return nil, fmt.Errorf("kafka producer create fail. err: %v", err)
	}

	return &AliyunProducer{
		producer: producer,
	}, err
}

// Produce produce new kafka message
func (producer *AliyunProducer) Produce(topic string, key []byte, content interface{}) error {

	data, err := json.Marshal(content)

	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = producer.producer.SendMessage(msg)

	if err != nil {
		return fmt.Errorf(
			"Kafka send message error. topic: %v. key: %v. content: %v\n\t%s",
			topic, hex.EncodeToString(key), content, err,
		)

	}

	return nil
}
