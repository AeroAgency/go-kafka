package consumer

import (
	adapterMocks "github.com/AeroAgency/go-kafka/adapter/mocks"
	"github.com/AeroAgency/go-kafka/consumer/mocks"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestKafkaConsumer_StartConsumer(t *testing.T) {
	type m struct {
		message   *mocks.Message
		consumer  *adapterMocks.Consumer
		factory   *mocks.KafkaConsumerFactory
		connector *mocks.KafkaConnector
	}

	configMap := &kafka.ConfigMap{}
	testTopic := "gotest"
	pollTimeoutMs := 10
	maxErrorsExitCount := 10

	testCases := []struct {
		name             string
		topics           []string
		message          interface{}
		expectedError    error
		interruptTimeout int
		timeout          int
		expectedFatal    bool
		on               func(mocks *m, doneChannel chan struct{})
		assert           func(t *testing.T, mocks *m)
	}{
		{
			name:    "Consuming messages without errors",
			topics:  []string{testTopic},
			timeout: 1000,
			on: func(mocks *m, doneChannel chan struct{}) {
				messagesSent := 0
				maxMessagesSent := 20
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.connector.On("GetPollTimeoutMs").
					Once().
					Return(pollTimeoutMs)

				mocks.connector.On("GetMaxErrorsExitCount").
					Once().
					Return(maxErrorsExitCount)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("SubscribeTopics", mock.Anything, mock.Anything).
					Once().
					Return(nil)

				mocks.consumer.On("Poll", pollTimeoutMs).
					Run(func(args mock.Arguments) {
						messagesSent++
					}).
					Return(&kafka.Message{
						Value: []byte("test message"),
						Headers: []kafka.Header{
							{
								Key:   "test header key",
								Value: []byte("test header value"),
							},
						},
					})

				mocks.consumer.On("Close").
					Once().
					Return(nil)

				mocks.message.On("Handle", mock.Anything).
					Run(func(args mock.Arguments) {
						if messagesSent == maxMessagesSent {
							doneChannel <- struct{}{}
						}
					}).
					Times(messagesSent).
					Return()
			},
		},
		{
			name:    "Consuming nil events",
			topics:  []string{testTopic},
			timeout: 1000,
			on: func(mocks *m, doneChannel chan struct{}) {
				messagesSent := 0
				maxMessagesSent := 20
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.connector.On("GetPollTimeoutMs").
					Once().
					Return(pollTimeoutMs)

				mocks.connector.On("GetMaxErrorsExitCount").
					Once().
					Return(maxErrorsExitCount)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("SubscribeTopics", mock.Anything, mock.Anything).
					Once().
					Return(nil)

				mocks.consumer.On("Poll", pollTimeoutMs).
					Run(func(args mock.Arguments) {
						messagesSent++
						if messagesSent == maxMessagesSent {
							doneChannel <- struct{}{}
						}
					}).
					Return(nil)

				mocks.consumer.On("Close").
					Once().
					Return(nil)
			},
		},
		{
			name:    "Consuming default case events",
			topics:  []string{testTopic},
			timeout: 1000,
			on: func(mocks *m, doneChannel chan struct{}) {
				messagesSent := 0
				maxMessagesSent := 20
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.connector.On("GetPollTimeoutMs").
					Once().
					Return(pollTimeoutMs)

				mocks.connector.On("GetMaxErrorsExitCount").
					Once().
					Return(maxErrorsExitCount)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("SubscribeTopics", mock.Anything, mock.Anything).
					Once().
					Return(nil)

				mocks.consumer.On("Poll", pollTimeoutMs).
					Run(func(args mock.Arguments) {
						messagesSent++
						if messagesSent == maxMessagesSent {
							doneChannel <- struct{}{}
						}
					}).
					Return(&kafka.AssignedPartitions{})

				mocks.consumer.On("Close").
					Once().
					Return(nil)
			},
		},
		{
			name:          "Fatal when NewConsumer produce error",
			expectedFatal: true,
			timeout:       1000,
			on: func(mocks *m, doneChannel chan struct{}) {
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(nil, kafka.NewError(kafka.ErrFail, "fail", true))
			},
		},
		{
			name:          "Fatal when topics length is zero",
			expectedFatal: true,
			timeout:       1000,
			on: func(mocks *m, doneChannel chan struct{}) {
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)
			},
		},
		{
			name:          "Fatal when subscribe topics produce error",
			expectedFatal: true,
			topics:        []string{testTopic},
			timeout:       1000,
			on: func(mocks *m, doneChannel chan struct{}) {
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("SubscribeTopics", mock.Anything, mock.Anything).
					Once().
					Return(kafka.NewError(kafka.ErrFail, "fail", true))
			},
		},
		{
			name:    "Stop after maxErrorExitCount",
			topics:  []string{testTopic},
			timeout: 1000,

			on: func(mocks *m, doneChannel chan struct{}) {
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.connector.On("GetPollTimeoutMs").
					Once().
					Return(pollTimeoutMs)

				mocks.connector.On("GetMaxErrorsExitCount").
					Once().
					Return(maxErrorsExitCount)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("SubscribeTopics", mock.Anything, mock.Anything).
					Once().
					Return(nil)

				mocks.consumer.On("Poll", pollTimeoutMs).
					Times(maxErrorsExitCount).
					Return(kafka.Error{})

				mocks.consumer.On("Close").
					Once().
					Return(nil)
			},
		},
		{
			name:    "Stop after errAllBrokersDown",
			topics:  []string{testTopic},
			timeout: 1000,

			on: func(mocks *m, doneChannel chan struct{}) {
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.connector.On("GetPollTimeoutMs").
					Once().
					Return(pollTimeoutMs)

				mocks.connector.On("GetMaxErrorsExitCount").
					Once().
					Return(maxErrorsExitCount)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("SubscribeTopics", mock.Anything, mock.Anything).
					Once().
					Return(nil)

				mocks.consumer.On("Poll", pollTimeoutMs).
					Once().
					Return(kafka.NewError(kafka.ErrAllBrokersDown, kafka.ErrAllBrokersDown.String(), false))

				mocks.consumer.On("Close").
					Once().
					Return(nil)
			},
		},
		{
			name:    "Reset errorsExitCnt after message consuming",
			topics:  []string{testTopic},
			timeout: 1000,

			on: func(mocks *m, doneChannel chan struct{}) {
				messagesSent := 0
				errorsSent := 0
				maxMessagesSent := 20
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.connector.On("GetPollTimeoutMs").
					Once().
					Return(pollTimeoutMs)

				mocks.connector.On("GetMaxErrorsExitCount").
					Once().
					Return(maxErrorsExitCount)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("SubscribeTopics", mock.Anything, mock.Anything).
					Once().
					Return(nil)

				mocks.consumer.On("Poll", pollTimeoutMs).
					Return(func(int) kafka.Event {
						if errorsSent == maxErrorsExitCount-1 {
							messagesSent++
							errorsSent = 0
							return &kafka.Message{}
						}
						errorsSent++
						return kafka.Error{}
					})

				mocks.consumer.On("Close").
					Once().
					Return(nil)

				mocks.message.On("Handle", mock.Anything).
					Run(func(args mock.Arguments) {
						if messagesSent == maxMessagesSent {
							doneChannel <- struct{}{}
						}
					}).
					Times(messagesSent).
					Return()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := &m{
				message:   mocks.NewMessage(t),
				connector: mocks.NewKafkaConnector(t),
				factory:   mocks.NewKafkaConsumerFactory(t),
				consumer:  adapterMocks.NewConsumer(t),
			}

			doneChannel := make(chan struct{})

			if tc.on != nil {
				tc.on(m, doneChannel)
			}

			logger, _ := test.NewNullLogger()
			logger.ExitFunc = func(i int) {
				panic(i)
			}

			consumer := &KafkaConsumer{
				Message:        m.message,
				KafkaConnector: m.connector,
				Logger:         logger,
				factory:        m.factory,
			}

			wg := sync.WaitGroup{}
			wg.Add(1)

			go func() {
				defer wg.Done()
				p, err := os.FindProcess(os.Getpid())
				if err != nil {
					panic(err)
				}

				go processInterrupt(t, p, doneChannel, tc.timeout)

				if tc.expectedFatal {
					assert.Panics(t, func() {
						consumer.StartConsumer(tc.topics...)
					})
				} else {
					consumer.StartConsumer(tc.topics...)
				}

			}()
			wg.Wait()

			if tc.assert != nil {
				tc.assert(t, m)
			}
		})
	}
}

func processInterrupt(t *testing.T, p *os.Process, doneChannel chan struct{}, timeout int) {
	select {
	case <-doneChannel:
	case <-time.After(time.Millisecond * time.Duration(timeout)):
		t.Error("Timed out")
	}
	err := p.Signal(syscall.SIGINT)
	if err != nil {
		panic(err)
	}
}

func TestKafkaConsumer_CheckConsumer(t *testing.T) {
	type m struct {
		consumer  *adapterMocks.Consumer
		factory   *mocks.KafkaConsumerFactory
		connector *mocks.KafkaConnector
	}
	testTopic := "gotest"

	configMap := &kafka.ConfigMap{}
	topics := make(map[string]kafka.TopicMetadata)
	topics[testTopic] = kafka.TopicMetadata{}
	metadata := &kafka.Metadata{
		Brokers:           nil,
		Topics:            topics,
		OriginatingBroker: kafka.BrokerMetadata{},
	}

	testCases := []struct {
		name              string
		expectedFatal     bool
		expectedError     error
		expectedTopicsLen int
		on                func(mocks *m)
		assert            func(t *testing.T, mocks *m)
	}{
		{
			name:              "Check consumer without errors",
			expectedTopicsLen: len(topics),
			on: func(mocks *m) {
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("GetMetadata", mock.Anything, true, mock.AnythingOfType("int")).
					Once().
					Return(metadata, nil)

				mocks.consumer.On("Close").
					Once().
					Return(nil)
			},
		},
		{
			name:          "Fatal when GetMetadata produce error",
			expectedFatal: true,
			on: func(mocks *m) {
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(mocks.consumer, nil)

				mocks.consumer.On("GetMetadata", mock.Anything, true, mock.AnythingOfType("int")).
					Once().
					Return(nil, kafka.NewError(kafka.ErrFail, "fail", true))
			},
		},
		{
			name:          "Fatal when NewConsumer produce error",
			expectedFatal: true,
			on: func(mocks *m) {
				mocks.connector.On("GetConsumerConfigMap").
					Once().
					Return(configMap)

				mocks.factory.On("NewConsumer", configMap).
					Once().
					Return(nil, kafka.NewError(kafka.ErrFail, "fail", true))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := &m{
				connector: mocks.NewKafkaConnector(t),
				factory:   mocks.NewKafkaConsumerFactory(t),
				consumer:  adapterMocks.NewConsumer(t),
			}

			if tc.on != nil {
				tc.on(m)
			}

			logger, _ := test.NewNullLogger()
			logger.ExitFunc = func(i int) {
				panic(i)
			}

			consumer := &KafkaConsumer{
				KafkaConnector: m.connector,
				Logger:         logger,
				factory:        m.factory,
			}

			if tc.expectedFatal {
				assert.Panics(t, func() {
					_, _ = consumer.CheckConsumer()
				})
			} else {
				topicsLen, err := consumer.CheckConsumer()
				assert.Equalf(t, tc.expectedTopicsLen, topicsLen, "Expected topicsLen: %v, Actual topicsLen: %v", tc.expectedTopicsLen, topicsLen)
				if tc.expectedError != nil {
					assert.ErrorAsf(t, err, &tc.expectedError, "Expected error: %v, Actual error: %v", tc.expectedError, err)
				} else {
					assert.NoError(t, err, "Expected no errors")
				}
			}

			if tc.assert != nil {
				tc.assert(t, m)
			}
		})
	}
}

func TestKafkaConsumer_SetLogger(t *testing.T) {
	logger, _ := test.NewNullLogger()

	mockKafkaConnector := mocks.NewKafkaConnector(t)
	mockKafkaConnector.On("SetLogger", logger).
		Once().
		Return()

	consumer := &KafkaConsumer{
		KafkaConnector: mockKafkaConnector,
	}

	consumer.SetLogger(logger)

	assert.Equal(t, consumer.Logger, logger)

}
