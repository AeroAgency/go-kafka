package producer

import (
	"encoding/json"
	connector "github.com/AeroAgency/go-kafka"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

type KafkaProducer struct {
	KafkaConnector connector.KafkaConnector
}

func NewKafkaProducer() *KafkaProducer {
	return &KafkaProducer{
		connector.KafkaConnector{},
	}
}
func (k *KafkaProducer) CreateProducer() *kafka.Producer {
	p, err := kafka.NewProducer(k.KafkaConnector.GetConfigMap())
	if err != nil {
		log.Errorf("Kafka Producer: Failed to create producer: %s\n", err)
		//os.Exit(1)
	}
	log.Info("Kafka Producer: Created Producer %v\n", p)
	return p
}

func (k *KafkaProducer) SendMessage(topic string, value interface{}) {
	p := k.CreateProducer()
	deliveryChan := make(chan kafka.Event)

	message, err := json.Marshal(&value)
	if err != nil {
		log.Errorf("Kafka Producer: send message error: %s\n", err)
	}
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
		//Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)
	if err != nil {
		log.Errorf("Kafka Producer: send message error: %s\n", err)
	}
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Errorf("Kafka Producer: Delivery failed: %v\n", m.TopicPartition.Error)
	}

	close(deliveryChan)
}
