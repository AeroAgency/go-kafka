package producer

import (
	"encoding/json"
	connector "github.com/AeroAgency/go-kafka"
	"github.com/AeroAgency/go-kafka/adapters"
	"github.com/AeroAgency/go-kafka/adapters/confluent"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

//go:generate go run github.com/vektra/mockery/v2 --name KafkaConnector
type KafkaConnector interface {
	SetLogger(logger log.FieldLogger)
	GetProducerConfigMap() *kafka.ConfigMap
}

//go:generate go run github.com/vektra/mockery/v2 --name KafkaProducerFactory
type KafkaProducerFactory interface {
	NewProducer(configMap *kafka.ConfigMap) (adapters.Producer, error)
}

type KafkaProducer struct {
	KafkaConnector KafkaConnector
	Logger         log.FieldLogger
	factory        KafkaProducerFactory
}

func NewKafkaProducer() *KafkaProducer {
	return &KafkaProducer{
		connector.NewKafkaConnector(),
		log.New(),
		confluent.KafkaFactory{},
	}
}

func (k *KafkaProducer) SetLogger(logger log.FieldLogger) {
	k.Logger = logger
	k.KafkaConnector.SetLogger(logger)
}

func (k *KafkaProducer) CreateProducer() adapters.Producer {
	p, err := k.factory.NewProducer(k.KafkaConnector.GetProducerConfigMap())
	if err != nil {
		k.Logger.Fatalf("Kafka Producer: Failed to create producer: %s", err)
	}
	k.Logger.Debugf("Kafka Producer: Created Producer %v", p)
	return p
}

func (k *KafkaProducer) SendMessage(topic string, value interface{}) error {
	message, err := json.Marshal(&value)
	if err != nil {
		k.Logger.Errorf("Kafka Producer: send message error: %s", err)
		return err
	}

	return k.SendRawMessage(topic, message, []kafka.Header{})
}

func (k *KafkaProducer) SendRawMessage(topic string, message []byte, headers []kafka.Header) error {
	return k.sendRawMessage(topic, nil, kafka.PartitionAny, message, headers)
}

func (k *KafkaProducer) SendRawMsg(topic string, key []byte, partition int32, message []byte, headers []kafka.Header) error {
	return k.sendRawMessage(topic, key, partition, message, headers)
}

func (k *KafkaProducer) sendRawMessage(topic string, key []byte, partition int32, message []byte, headers []kafka.Header) error {
	p := k.CreateProducer() //todo: need to create producer for every message?
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	defer p.Close()

	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
		Key:            key,
		Value:          message,
		Headers:        headers,
	}, deliveryChan)
	if err != nil {
		k.Logger.Errorf("Kafka Producer: send message error: %s", err)
		return err
	}
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		k.Logger.Errorf("Kafka Producer: Delivery failed: %v", m.TopicPartition.Error)
		return m.TopicPartition.Error
	}
	return nil
}
