package adapters

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//go:generate go run github.com/vektra/mockery/v2 --name Producer
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

//go:generate go run github.com/vektra/mockery/v2 --name Consumer
type Consumer interface {
	SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error
	Poll(timeoutMs int) kafka.Event
	GetMetadata(topic *string, allTopics bool, timeoutMs int) (*kafka.Metadata, error)
	Close() error
	String() string
}

type RebalanceCb func(Consumer, kafka.Event) error
