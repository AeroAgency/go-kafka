package consumer

import (
	"github.com/AeroAgency/go-kafka/adapter"
	"github.com/AeroAgency/go-kafka/adapter/confluent"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"

	connector "github.com/AeroAgency/go-kafka"
)

// Message Интерфейс работы с сообщениями
//
//go:generate go run github.com/vektra/mockery/v2 --name Message
type Message interface {
	// Handle Обработать сообщение
	Handle(msg kafka.Message)
}

//go:generate go run github.com/vektra/mockery/v2 --name KafkaConnector
type KafkaConnector interface {
	SetLogger(logger log.FieldLogger)
	GetConsumerConfigMap() *kafka.ConfigMap
	GetMaxErrorsExitCount() int
	GetPollTimeoutMs() int
}

//go:generate go run github.com/vektra/mockery/v2 --name KafkaConsumerFactory
type KafkaConsumerFactory interface {
	NewConsumer(configMap *kafka.ConfigMap) (adapter.Consumer, error)
}

type KafkaConsumer struct {
	Message        Message
	KafkaConnector KafkaConnector
	Logger         log.FieldLogger
	factory        KafkaConsumerFactory
}

func NewKafkaConsumer(message Message) *KafkaConsumer {
	return &KafkaConsumer{
		Message:        message,
		KafkaConnector: connector.NewKafkaConnector(),
		Logger:         log.New(),
		factory:        confluent.KafkaFactory{},
	}
}

func (k *KafkaConsumer) SetLogger(logger log.FieldLogger) {
	k.Logger = logger
	k.KafkaConnector.SetLogger(logger)
}

func (k *KafkaConsumer) StartConsumer(topics ...string) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	c, err := k.factory.NewConsumer(k.KafkaConnector.GetConsumerConfigMap())
	if err != nil {
		k.Logger.Fatalf("failed to start consumer: %v", err)
	}
	k.Logger.Infof("Created Consumer %v", c)
	if len(topics) == 0 {
		k.Logger.Fatalf("failed to start consumer: no topics provided")
	}

	var rebalanceCb func(c *kafka.Consumer, e kafka.Event) error
	rebalanceCb = func(c *kafka.Consumer, e kafka.Event) error {
		k.Logger.Infof("Got kafka partition rebalance event %s in %s consumer", e.String(), c.String())
		switch e.(type) {
		case kafka.RevokedPartitions:
			// Resubscribe to the topic to get new partitions assigned.
			err := c.SubscribeTopics(topics, rebalanceCb)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err = c.SubscribeTopics(topics, rebalanceCb)
	if err != nil {
		k.Logger.Fatalf("failed to subscribe topic: %v", err)
	}
	timeoutMs := k.KafkaConnector.GetPollTimeoutMs()
	errorsExitCntBase := k.KafkaConnector.GetMaxErrorsExitCount()
	errorsExitCnt := errorsExitCntBase

	defer func() {
		k.Logger.Infof("Closing consumer %v", c)
		_ = c.Close()
	}()

	for {
		select {
		case sig := <-sigchan:
			k.Logger.Infof("Caught signal %v: terminating", sig)
			return
		default:
			ev := c.Poll(timeoutMs)
			if ev == nil {
				continue
			}
			err := k.handleEvent(ev)
			if err == nil {
				errorsExitCnt = errorsExitCntBase
			} else {
				errorsExitCnt = k.handleErr(err, errorsExitCnt)

				if errorsExitCntBase > 0 && errorsExitCnt <= 0 {
					k.Logger.Errorf("Stop consuming with error: %+v", err)
					return
				}
				k.Logger.Errorf("Continue consuming with error: %+v", err)
			}
		}
	}
}

func (k *KafkaConsumer) handleEvent(ev kafka.Event) *kafka.Error {
	switch e := ev.(type) {
	case *kafka.Message:
		k.Logger.Debugf("Message on topic %s, partition: %s: %s", e.TopicPartition.Topic, e.TopicPartition, string(e.Value))
		if e.Headers != nil {
			k.Logger.Debugf("Headers: %+v", e.Headers)
		}
		k.Message.Handle(*e)
	case kafka.Error:
		return &e
	default:
		k.Logger.Infof("Ignored %+v", e)
	}
	return nil
}

func (k *KafkaConsumer) handleErr(err *kafka.Error, errorsExitCnt int) int {
	if err.Code() == kafka.ErrAllBrokersDown {
		return 0
	}

	errorsExitCnt--
	return errorsExitCnt
}

func (k *KafkaConsumer) CheckConsumer() (int, error) {
	c, err := k.factory.NewConsumer(k.KafkaConnector.GetConsumerConfigMap())
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

	err = c.Close()
	if err != nil {
		return 0, err
	}
	return topicLen, err
}
