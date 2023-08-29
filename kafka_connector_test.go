package connector

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestKafkaConnector_GetConfigMap(t *testing.T) {
	kafkaUrl := "testKafkaUrl"
	kafkaMessageMaxBytes := 100
	kafkaUsername := "testUser"
	kafkaPassword := "testPass"
	kafkaSecurityProtocol := "testProtocol"
	kafkaSaslMechanism := "testSasl"
	kafkaGroupId := "testGroupId"
	kafkaAutoOffsetReset := "testOffset"
	kafkaMaxPollIntervalMs := 10

	testCases := []struct {
		name             string
		forConsumer      bool
		want             *kafka.ConfigMap
		expectedFatal    bool
		expectedFatalLog string
		setEnv           func(t *testing.T)
	}{
		{
			name: "Get for producer",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
			},
		},
		{
			name:        "For consumer",
			forConsumer: true,
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
				"group.id":             kafkaGroupId,
				"auto.offset.reset":    kafkaAutoOffsetReset,
				"max.poll.interval.ms": kafkaMaxPollIntervalMs,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaGroupIdEnv, kafkaGroupId)
				t.Setenv(kafkaAutoOffsetResetEnv, kafkaAutoOffsetReset)
				t.Setenv(kafkaMaxPollIntervalMsEnv, strconv.Itoa(kafkaMaxPollIntervalMs))
			},
		},
		{
			name:        "For consumer with default values",
			forConsumer: true,
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
				"group.id":             kafkaGroupId,
				"auto.offset.reset":    defaultKafkaAutoOffsetReset,
				"max.poll.interval.ms": defaultKafkaMaxPollIntervalMs,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaGroupIdEnv, kafkaGroupId)
			},
		},
		{
			name: "With default values",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    defaultKafkaMaxMessageSize,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
			},
		},
		{
			name: "For producer with security config",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
				"security.protocol":    kafkaSecurityProtocol,
				"sasl.username":        kafkaUsername,
				"sasl.password":        kafkaPassword,
				"sasl.mechanism":       kafkaSaslMechanism,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:        "For consumer with security config",
			forConsumer: true,
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
				"auto.offset.reset":    kafkaAutoOffsetReset,
				"group.id":             kafkaGroupId,
				"max.poll.interval.ms": kafkaMaxPollIntervalMs,
				"security.protocol":    kafkaSecurityProtocol,
				"sasl.username":        kafkaUsername,
				"sasl.password":        kafkaPassword,
				"sasl.mechanism":       kafkaSaslMechanism,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaGroupIdEnv, kafkaGroupId)
				t.Setenv(kafkaAutoOffsetResetEnv, kafkaAutoOffsetReset)
				t.Setenv(kafkaMaxPollIntervalMsEnv, strconv.Itoa(kafkaMaxPollIntervalMs))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal when kafka url empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaUrlEnv),
		},
		{
			name:             "Fatal for consumer when group id empty",
			forConsumer:      true,
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaGroupIdEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaAutoOffsetResetEnv, kafkaAutoOffsetReset)
				t.Setenv(kafkaMaxPollIntervalMsEnv, strconv.Itoa(kafkaMaxPollIntervalMs))
			},
		},
		{
			name:             "Fatal with security config when password empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaPasswordEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal with security config when user empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaUsernameEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal with security config when sasl mechanism empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaSaslMechanismEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
			},
		},
		{
			name:             "Fatal with security config when security protocol empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaSecurityProtocolEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, loggerHook := test.NewNullLogger()
			logger.ExitFunc = func(i int) {
				panic(i)
			}

			if tc.setEnv != nil {
				tc.setEnv(t)
			}

			connector := &KafkaConnector{
				Logger: logger,
			}
			if tc.expectedFatal {
				assert.Panics(t, func() {
					connector.GetConfigMap(tc.forConsumer)
				})
				lastLoggerEntry := loggerHook.LastEntry()
				assert.Equal(t, logrus.FatalLevel, lastLoggerEntry.Level)
				assert.Equal(t, tc.expectedFatalLog, lastLoggerEntry.Message)

			} else {
				got := connector.GetConfigMap(tc.forConsumer)
				assert.NotNil(t, got)
				assert.Equal(t, got, tc.want)
			}
		})
	}
}

func TestKafkaConnector_GetProducerConfigMap(t *testing.T) {
	kafkaUrl := "testKafkaUrl"
	kafkaMessageMaxBytes := 100
	kafkaUsername := "testUser"
	kafkaPassword := "testPass"
	kafkaSecurityProtocol := "testProtocol"
	kafkaSaslMechanism := "testSasl"

	testCases := []struct {
		name             string
		want             *kafka.ConfigMap
		expectedFatal    bool
		expectedFatalLog string
		setEnv           func(t *testing.T)
	}{
		{
			name: "Without security config",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
			},
		},
		{
			name: "With default values",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    defaultKafkaMaxMessageSize,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
			},
		},
		{
			name: "With security config",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
				"security.protocol":    kafkaSecurityProtocol,
				"sasl.username":        kafkaUsername,
				"sasl.password":        kafkaPassword,
				"sasl.mechanism":       kafkaSaslMechanism,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal when kafka url empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaUrlEnv),
		},
		{
			name:             "Fatal with security config when password empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaPasswordEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal with security config when user empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaUsernameEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal with security config when sasl mechanism empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaSaslMechanismEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
			},
		},
		{
			name:             "Fatal with security config when security protocol empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaSecurityProtocolEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, loggerHook := test.NewNullLogger()
			logger.ExitFunc = func(i int) {
				panic(i)
			}

			if tc.setEnv != nil {
				tc.setEnv(t)
			}

			connector := &KafkaConnector{
				Logger: logger,
			}
			if tc.expectedFatal {
				assert.Panics(t, func() {
					connector.GetProducerConfigMap()
				})
				lastLoggerEntry := loggerHook.LastEntry()
				assert.Equal(t, logrus.FatalLevel, lastLoggerEntry.Level)
				assert.Equal(t, tc.expectedFatalLog, lastLoggerEntry.Message)

			} else {
				got := connector.GetProducerConfigMap()
				assert.NotNil(t, got)
				assert.Equal(t, got, tc.want)
			}
		})
	}
}

func TestKafkaConnector_GetConsumerConfigMap(t *testing.T) {
	kafkaUrl := "testKafkaUrl"
	kafkaMessageMaxBytes := 100
	kafkaUsername := "testUser"
	kafkaPassword := "testPass"
	kafkaSecurityProtocol := "testProtocol"
	kafkaSaslMechanism := "testSasl"
	kafkaGroupId := "testGroupId"
	kafkaAutoOffsetReset := "testOffset"
	kafkaMaxPollIntervalMs := 10

	testCases := []struct {
		name             string
		want             *kafka.ConfigMap
		expectedFatal    bool
		expectedFatalLog string
		setEnv           func(t *testing.T)
	}{
		{
			name: "Without security config",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
				"group.id":             kafkaGroupId,
				"auto.offset.reset":    kafkaAutoOffsetReset,
				"max.poll.interval.ms": kafkaMaxPollIntervalMs,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaGroupIdEnv, kafkaGroupId)
				t.Setenv(kafkaAutoOffsetResetEnv, kafkaAutoOffsetReset)
				t.Setenv(kafkaMaxPollIntervalMsEnv, strconv.Itoa(kafkaMaxPollIntervalMs))
			},
		},
		{
			name: "With default values",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    defaultKafkaMaxMessageSize,
				"group.id":             kafkaGroupId,
				"auto.offset.reset":    defaultKafkaAutoOffsetReset,
				"max.poll.interval.ms": defaultKafkaMaxPollIntervalMs,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaGroupIdEnv, kafkaGroupId)
			},
		},
		{
			name: "With security config",
			want: &kafka.ConfigMap{
				"metadata.broker.list": kafkaUrl,
				"message.max.bytes":    kafkaMessageMaxBytes,
				"auto.offset.reset":    kafkaAutoOffsetReset,
				"group.id":             kafkaGroupId,
				"max.poll.interval.ms": kafkaMaxPollIntervalMs,
				"security.protocol":    kafkaSecurityProtocol,
				"sasl.username":        kafkaUsername,
				"sasl.password":        kafkaPassword,
				"sasl.mechanism":       kafkaSaslMechanism,
			},
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaGroupIdEnv, kafkaGroupId)
				t.Setenv(kafkaAutoOffsetResetEnv, kafkaAutoOffsetReset)
				t.Setenv(kafkaMaxPollIntervalMsEnv, strconv.Itoa(kafkaMaxPollIntervalMs))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal when kafka url empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaUrlEnv),
		},
		{
			name:             "Fatal when group id empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaGroupIdEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaAutoOffsetResetEnv, kafkaAutoOffsetReset)
				t.Setenv(kafkaMaxPollIntervalMsEnv, strconv.Itoa(kafkaMaxPollIntervalMs))
			},
		},
		{
			name:             "Fatal with security config when password empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaPasswordEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal with security config when user empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaUsernameEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
		{
			name:             "Fatal with security config when sasl mechanism empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaSaslMechanismEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSecurityProtocolEnv, kafkaSecurityProtocol)
			},
		},
		{
			name:             "Fatal with security config when security protocol empty",
			expectedFatal:    true,
			expectedFatalLog: fmt.Sprintf(getEnvErrorLog, kafkaSecurityProtocolEnv),
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaUrlEnv, kafkaUrl)
				t.Setenv(kafkaMaxMessageSizeEnv, strconv.Itoa(kafkaMessageMaxBytes))
				t.Setenv(kafkaUsernameEnv, kafkaUsername)
				t.Setenv(kafkaPasswordEnv, kafkaPassword)
				t.Setenv(kafkaSaslMechanismEnv, kafkaSaslMechanism)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, loggerHook := test.NewNullLogger()
			logger.ExitFunc = func(i int) {
				panic(i)
			}

			if tc.setEnv != nil {
				tc.setEnv(t)
			}

			connector := &KafkaConnector{
				Logger: logger,
			}
			if tc.expectedFatal {
				assert.Panics(t, func() {
					connector.GetConsumerConfigMap()
				})
				lastLoggerEntry := loggerHook.LastEntry()
				assert.Equal(t, logrus.FatalLevel, lastLoggerEntry.Level)
				assert.Equal(t, tc.expectedFatalLog, lastLoggerEntry.Message)

			} else {
				got := connector.GetConsumerConfigMap()
				assert.NotNil(t, got)
				assert.Equal(t, got, tc.want)
			}
		})
	}
}

func TestKafkaConnector_GetMaxErrorsExitCount(t *testing.T) {
	maxErrorsExitCount := 10

	testCases := []struct {
		name   string
		want   int
		setEnv func(t *testing.T)
	}{
		{
			name: "Get",
			want: maxErrorsExitCount,
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaErrorsExitCountEnv, strconv.Itoa(maxErrorsExitCount))
			},
		},
		{
			name: "Get default",
			want: defaultErrorsExitCount,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setEnv != nil {
				tc.setEnv(t)
			}

			connector := &KafkaConnector{}
			got := connector.GetMaxErrorsExitCount()
			assert.NotNil(t, got)
			assert.Equal(t, got, tc.want)
		})
	}
}

func TestKafkaConnector_GetPollTimeoutMs(t *testing.T) {
	pollTimeoutMs := 10

	testCases := []struct {
		name   string
		want   int
		setEnv func(t *testing.T)
	}{
		{
			name: "Get",
			want: pollTimeoutMs,
			setEnv: func(t *testing.T) {
				t.Setenv(kafkaTimeoutMsEnv, strconv.Itoa(pollTimeoutMs))
			},
		},
		{
			name: "Get default",
			want: defaultTimeoutMs,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setEnv != nil {
				tc.setEnv(t)
			}

			connector := &KafkaConnector{}
			got := connector.GetPollTimeoutMs()
			assert.NotNil(t, got)
			assert.Equal(t, got, tc.want)
		})
	}
}

func TestKafkaConnector_SetLogger(t *testing.T) {
	logger, _ := test.NewNullLogger()

	connector := &KafkaConnector{}

	connector.SetLogger(logger)

	assert.Equal(t, connector.Logger, logger)
}

func TestNewKafkaConnector(t *testing.T) {
	connector := NewKafkaConnector()
	assert.NotNil(t, connector)
}
