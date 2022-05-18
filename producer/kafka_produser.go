package producer

import (
	"encoding/json"
	connector "github.com/AeroAgency/go-kafka"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

type KafkaProducer struct {
	KafkaConnector connector.KafkaConnector
	Logger         log.FieldLogger
}

func NewKafkaProducer() *KafkaProducer {
	return &KafkaProducer{
		*connector.NewKafkaConnector(),
		log.New(),
	}
}

func (k *KafkaProducer) SetLogger(logger log.FieldLogger) {
	k.Logger = logger
	k.KafkaConnector.Logger = logger
}

func (k *KafkaProducer) CreateProducer() *kafka.Producer {
	p, err := kafka.NewProducer(k.KafkaConnector.GetConfigMap(false))
	if err != nil {
		k.Logger.Fatalf("Kafka Producer: Failed to create producer: %s", err)
	}
	k.Logger.Infof("Kafka Producer: Created Producer %v", p)
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
	p := k.CreateProducer()
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	defer p.Close()

	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
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
