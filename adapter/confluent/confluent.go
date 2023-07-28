package confluent

import (
	"github.com/AeroAgency/go-kafka/adapter"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaFactory struct{}

func (f KafkaFactory) NewProducer(configMap *kafka.ConfigMap) (adapter.Producer, error) {
	return kafka.NewProducer(configMap)
}

func (f KafkaFactory) NewConsumer(configMap *kafka.ConfigMap) (adapter.Consumer, error) {
	return kafka.NewConsumer(configMap)
}
