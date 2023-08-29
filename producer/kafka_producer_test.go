package producer

import (
	"encoding/json"
	adapterMocks "github.com/AeroAgency/go-kafka/adapters/mocks"
	producerMocks "github.com/AeroAgency/go-kafka/producer/mocks"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestKafkaProducer_CreateProducer(t *testing.T) {
	type m struct {
		producer  *adapterMocks.Producer
		factory   *producerMocks.KafkaProducerFactory
		connector *producerMocks.KafkaConnector
	}

	configMap := &kafka.ConfigMap{
		"socket.timeout.ms":         100,
		"message.timeout.ms":        100,
		"go.delivery.report.fields": "key,value,headers",
	}

	testCases := []struct {
		name          string
		expectedFatal bool
		on            func(mocks *m)
		assert        func(t *testing.T, mocks *m)
	}{
		{
			name: "Create producer without error",
			on: func(mocks *m) {
				mocks.connector.On("GetProducerConfigMap").
					Once().
					Return(configMap)

				mocks.factory.On("NewProducer", configMap).
					Once().
					Return(mocks.producer, nil)
			},
		},
		{
			name:          "Create producer with fatal error",
			expectedFatal: true,
			on: func(mocks *m) {
				mocks.connector.On("GetProducerConfigMap").
					Once().
					Return(configMap)

				mocks.factory.On("NewProducer", configMap).
					Once().
					Return(nil, kafka.NewError(kafka.ErrFail, "fail", true))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := &m{
				producer:  adapterMocks.NewProducer(t),
				factory:   producerMocks.NewKafkaProducerFactory(t),
				connector: producerMocks.NewKafkaConnector(t),
			}

			if tc.on != nil {
				tc.on(m)
			}

			logger, _ := test.NewNullLogger()
			logger.ExitFunc = func(i int) {
				panic(1)
			}

			producer := &KafkaProducer{
				KafkaConnector: m.connector,
				Logger:         logger,
				factory:        m.factory,
			}

			if tc.expectedFatal {
				assert.Panics(t, func() {
					producer.CreateProducer()
				})
			} else {
				p := producer.CreateProducer()
				assert.NotNil(t, p, "CreateProducer shouldn't return nil")
				assert.Equal(t, m.producer, p)
			}

			if tc.assert != nil {
				tc.assert(t, m)
			}
		})
	}
}

func TestKafkaProducer_SendMessage(t *testing.T) {
	type m struct {
		producer  *adapterMocks.Producer
		factory   *producerMocks.KafkaProducerFactory
		connector *producerMocks.KafkaConnector
	}
	configMap := &kafka.ConfigMap{
		"socket.timeout.ms":         100,
		"message.timeout.ms":        100,
		"go.delivery.report.fields": "key,value,headers",
	}
	testTopic := "gotest"
	_ = "gotest_topic_error"

	validMessage := "test message"
	invalidMessage := make(chan int)

	testCases := []struct {
		name          string
		topic         string
		message       interface{}
		expectedError error
		on            func(mocks *m)
		assert        func(t *testing.T, mocks *m)
	}{
		{
			name:          "Successful message delivery",
			topic:         testTopic,
			message:       validMessage,
			expectedError: nil,
			on: func(mocks *m) {
				mocks.connector.On("GetProducerConfigMap").
					Once().
					Return(configMap)

				mocks.producer.On("Produce", mock.AnythingOfType("*kafka.Message"), mock.Anything).
					Run(func(args mock.Arguments) {
						deliveryChan := args.Get(1).(chan kafka.Event)
						message := &kafka.Message{}
						go func() {
							deliveryChan <- message
						}()
					}).
					Once().
					Return(nil)

				mocks.producer.On("Close").Once().Return()

				mocks.factory.On("NewProducer", configMap).
					Return(mocks.producer, nil)
			},
		},
		{
			name:          "Json marshall error",
			topic:         testTopic,
			message:       invalidMessage,
			expectedError: &json.UnsupportedTypeError{},
		},
		{
			name:          "Produce message error",
			topic:         testTopic,
			message:       validMessage,
			expectedError: &kafka.Error{},
			on: func(mocks *m) {
				mocks.connector.On("GetProducerConfigMap").
					Once().
					Return(configMap)

				mocks.producer.On("Produce", mock.AnythingOfType("*kafka.Message"), mock.Anything).
					Once().
					Return(kafka.Error{})

				mocks.producer.On("Close").Once().Return()

				mocks.factory.On("NewProducer", configMap).
					Return(mocks.producer, nil)
			},
		},
		{
			name:          "Delivery message error",
			topic:         testTopic,
			message:       validMessage,
			expectedError: &kafka.Error{},
			on: func(mocks *m) {
				mocks.connector.On("GetProducerConfigMap").Return(
					configMap,
				)

				mocks.producer.On("Produce", mock.AnythingOfType("*kafka.Message"), mock.Anything).
					Run(func(args mock.Arguments) {
						deliveryChan := args.Get(1).(chan kafka.Event)
						message := &kafka.Message{
							TopicPartition: kafka.TopicPartition{Error: kafka.NewError(kafka.ErrUnknownTopic, "err", false)},
						}
						go func() {
							deliveryChan <- message
						}()
					}).
					Once().
					Return(nil)

				mocks.producer.On("Close").Once().Return()

				mocks.factory.On("NewProducer", configMap).
					Return(mocks.producer, nil)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := &m{
				producer:  adapterMocks.NewProducer(t),
				factory:   producerMocks.NewKafkaProducerFactory(t),
				connector: producerMocks.NewKafkaConnector(t),
			}

			if tc.on != nil {
				tc.on(m)
			}

			logger, _ := test.NewNullLogger()
			logger.ExitFunc = func(i int) {}

			producer := &KafkaProducer{
				KafkaConnector: m.connector,
				Logger:         logger,
				factory:        m.factory,
			}

			err := producer.SendMessage(tc.topic, tc.message)

			if tc.expectedError != nil {
				assert.ErrorAsf(t, err, &tc.expectedError, "Expected error: %v, Actual error: %v", tc.expectedError, err)
			} else {
				assert.NoError(t, err, "Expected no errors")
			}

			if tc.assert != nil {
				tc.assert(t, m)
			}
		})
	}
}

func TestKafkaProducer_SendRawMsg(t *testing.T) {
	type m struct {
		producer  *adapterMocks.Producer
		factory   *producerMocks.KafkaProducerFactory
		connector *producerMocks.KafkaConnector
	}
	configMap := &kafka.ConfigMap{
		"socket.timeout.ms":         100,
		"message.timeout.ms":        100,
		"go.delivery.report.fields": "key,value,headers",
	}
	testTopic := "gotest"
	_ = "gotest_topic_error"

	validMessage := []byte("test message")

	testCases := []struct {
		name          string
		topic         string
		message       []byte
		expectedError error
		on            func(mocks *m)
		assert        func(t *testing.T, mocks *m)
	}{
		{
			name:          "Successful message delivery",
			topic:         testTopic,
			message:       validMessage,
			expectedError: nil,
			on: func(mocks *m) {
				mocks.connector.On("GetProducerConfigMap").
					Once().
					Return(configMap)

				mocks.producer.On("Produce", mock.AnythingOfType("*kafka.Message"), mock.Anything).
					Run(func(args mock.Arguments) {
						deliveryChan := args.Get(1).(chan kafka.Event)
						message := &kafka.Message{}
						go func() {
							deliveryChan <- message
						}()
					}).
					Once().
					Return(nil)

				mocks.producer.On("Close").Once().Return()

				mocks.factory.On("NewProducer", configMap).
					Return(mocks.producer, nil)
			},
		},
		{
			name:          "Produce message error",
			topic:         testTopic,
			message:       validMessage,
			expectedError: &kafka.Error{},
			on: func(mocks *m) {
				mocks.connector.On("GetProducerConfigMap").
					Once().
					Return(configMap)

				mocks.producer.On("Produce", mock.AnythingOfType("*kafka.Message"), mock.Anything).
					Once().
					Return(kafka.Error{})

				mocks.producer.On("Close").Once().Return()

				mocks.factory.On("NewProducer", configMap).
					Return(mocks.producer, nil)
			},
		},
		{
			name:          "Delivery message error",
			topic:         testTopic,
			message:       validMessage,
			expectedError: &kafka.Error{},
			on: func(mocks *m) {
				mocks.connector.On("GetProducerConfigMap").
					Return(configMap)

				mocks.producer.On("Produce", mock.AnythingOfType("*kafka.Message"), mock.Anything).
					Run(func(args mock.Arguments) {
						deliveryChan := args.Get(1).(chan kafka.Event)
						message := &kafka.Message{
							TopicPartition: kafka.TopicPartition{Error: kafka.NewError(kafka.ErrUnknownTopic, "err", false)},
						}
						go func() {
							deliveryChan <- message
						}()
					}).
					Once().
					Return(nil)

				mocks.producer.On("Close").
					Once().
					Return()

				mocks.factory.On("NewProducer", configMap).
					Return(mocks.producer, nil)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m := &m{
				producer:  adapterMocks.NewProducer(t),
				factory:   producerMocks.NewKafkaProducerFactory(t),
				connector: producerMocks.NewKafkaConnector(t),
			}

			if tc.on != nil {
				tc.on(m)
			}

			logger, _ := test.NewNullLogger()
			logger.ExitFunc = func(i int) {}

			producer := &KafkaProducer{
				KafkaConnector: m.connector,
				Logger:         logger,
				factory:        m.factory,
			}

			err := producer.SendRawMsg(tc.topic, nil, kafka.PartitionAny, tc.message, []kafka.Header{})

			if tc.expectedError != nil {
				assert.ErrorAsf(t, err, &tc.expectedError, "Expected error: %v, Actual error: %v", tc.expectedError, err)
			} else {
				assert.NoError(t, err, "Expected no errors")
			}

			if tc.assert != nil {
				tc.assert(t, m)
			}
		})
	}
}

func TestKafkaProducer_SetLogger(t *testing.T) {
	logger, _ := test.NewNullLogger()

	mockKafkaConnector := producerMocks.NewKafkaConnector(t)
	mockKafkaConnector.On("SetLogger", logger).
		Once().
		Return()

	consumer := &KafkaProducer{
		KafkaConnector: mockKafkaConnector,
	}

	consumer.SetLogger(logger)

	assert.Equal(t, consumer.Logger, logger)

}

func TestNewKafkaProducer(t *testing.T) {
	producer := NewKafkaProducer()
	assert.NotNil(t, producer)
}
