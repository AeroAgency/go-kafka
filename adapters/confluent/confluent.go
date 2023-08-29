package confluent

import (
	"github.com/AeroAgency/go-kafka/adapters"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaFactory struct{}

func (f KafkaFactory) NewProducer(configMap *kafka.ConfigMap) (adapters.Producer, error) {
	return kafka.NewProducer(configMap)
}

func (f KafkaFactory) NewConsumer(configMap *kafka.ConfigMap) (adapters.Consumer, error) {
	c, err := kafka.NewConsumer(configMap)
	return &Consumer{c}, err
}

type Consumer struct {
	*kafka.Consumer
}

func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb adapters.RebalanceCb) error {
	return c.Consumer.SubscribeTopics(topics, func(consumer *kafka.Consumer, event kafka.Event) error {
		return rebalanceCb(c, event)
	})
}
