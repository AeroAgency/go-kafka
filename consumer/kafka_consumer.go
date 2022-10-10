package consumer

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"

	connector "github.com/AeroAgency/go-kafka"
)

type KafkaConsumer struct {
	Message        Message
	KafkaConnector connector.KafkaConnector
	Logger         log.FieldLogger
}

func NewKafkaConsumer(message Message) *KafkaConsumer {
	return &KafkaConsumer{
		message,
		*connector.NewKafkaConnector(),
		log.New(),
	}
}

func (k *KafkaConsumer) SetLogger(logger log.FieldLogger) {
	k.Logger = logger
	k.KafkaConnector.Logger = logger
}

func (k *KafkaConsumer) StartConsumer(topics ...string) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	c, err := kafka.NewConsumer(k.KafkaConnector.GetConfigMap(true))
	if err != nil {
		k.Logger.Fatalf("failed to start consumer: %v", err)
	}
	k.Logger.Infof("Created Consumer %v", c)
	if len(topics) == 0 {
		k.Logger.Fatalf("failed to start consumer: can't get KAFKA_TOPIC param")
	}
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		k.Logger.Fatalf("failed to subscribe topic: %v", err)
	}
	run := true
	timeoutMs := k.KafkaConnector.GetPollTimeoutMs()
	errorsExitCntBase := k.KafkaConnector.GetMaxErrorsExitCount()
	errorsExitCnt := errorsExitCntBase

	for run == true {
		select {
		case sig := <-sigchan:
			k.Logger.Infof("Caught signal %v: terminating", sig)
			run = false
		default:
			ev := c.Poll(timeoutMs)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				errorsExitCnt = errorsExitCntBase
				k.Logger.Debugf("Message on topic %s, partition: %s: %s", strings.Join(topics, ", "), e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					k.Logger.Debugf("Headers: %+v", e.Headers)
				}
				k.Message.Handle(*e)
			case kafka.Error:
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				} else if errorsExitCntBase > 0 {
					errorsExitCnt--
					if errorsExitCnt == 0 {
						run = false
					}
				}
				if !run {
					k.Logger.Errorf("Stop consuming with error: %+v", e)
				} else {
					k.Logger.Errorf("Continue consuming with error: %+v", e)
				}
			default:
				k.Logger.Infof("Ignored %+v", e)
			}
		}
	}

	k.Logger.Infof("Closing consumer %v", c)
	_ = c.Close()
}

func (k *KafkaConsumer) CheckConsumer() (int, error) {
	c, err := kafka.NewConsumer(k.KafkaConnector.GetConfigMap(true))
	if err != nil {
		k.Logger.Fatalf("failed to start consumer: %v", err)
		return 0, err
	}
	metadata, err := c.GetMetadata(nil, true, 500)
	topicLen := 0
	if err != nil {
		k.Logger.Fatalf("failed to subscribe topic: %v", err)
	} else {
		topicLen = len(metadata.Topics)
	}

	c.Close()
	return topicLen, err
}