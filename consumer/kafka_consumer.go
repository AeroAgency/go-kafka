package consumer

import (
	"fmt"
	connector "github.com/AeroAgency/go-kafka"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

type KafkaConsumer struct {
	Message        Message
	KafkaConnector connector.KafkaConnector
}

func NewKafkaConsumer(message Message) *KafkaConsumer {
	return &KafkaConsumer{
		message,
		connector.KafkaConnector{},
	}
}
func (k *KafkaConsumer) StartConsumer() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	c, err := kafka.NewConsumer(k.KafkaConnector.GetConfigMap())
	if err != nil {
		log.Fatalf("failed to start consumer: %v", err)
		os.Exit(1)
	}
	log.Info("Created Consumer %v\n", c)
	topic, ok := os.LookupEnv("KAFKA_TOPIC")
	if !ok {
		log.Fatalf("failed to start consumer: can't get KAFKA_TOPIC param")
	}
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Errorf("failed to subscribe topic: %v", err)
		os.Exit(1)
	}
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			log.Info("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				log.Info("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					log.Info("%% Headers: %v\n", e.Headers)
				}
				log.Info("Message on %s: %s", topic, e.TopicPartition)
				k.Message.Handle(*e)
			case kafka.Error:
				fmt.Errorf("%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				log.Info("Ignored %v\n", e)
			}
		}
	}

	log.Info("Closing consumer\n")
	c.Close()
}
