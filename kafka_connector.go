package connector

import (
	"github.com/AeroAgency/go-kafka/env"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

const (
	kafkaUrlEnv               = "KAFKA_URL"
	kafkaSecurityProtocolEnv  = "KAFKA_SECURITY_PROTOCOL"
	kafkaSaslMechanismEnv     = "KAFKA_SASL_MECHANISM"
	kafkaUsernameEnv          = "KAFKA_USERNAME"
	kafkaPasswordEnv          = "KAFKA_PASSWORD"
	kafkaGroupIdEnv           = "KAFKA_GROUP_ID"
	kafkaMaxMessageSizeEnv    = "KAFKA_MESSAGE_MAX_BYTES"
	kafkaAutoOffsetResetEnv   = "KAFKA_AUTO_OFFSET_RESET"
	kafkaMaxPollIntervalMsEnv = "KAFKA_MAX_POLL_INTERVAL_MS"
	TimeoutMsEnv              = "KAFKA_POLL_TIMEOUT_MS"
	ErrorsExitCountEnv        = "KAFKA_MAX_ERRORS_EXIT_COUNT"
)

const (
	defaultKafkaMaxMessageSize    = 1048576
	defaultKafkaAutoOffsetReset   = "earliest"
	defaultKafkaMaxPollIntervalMs = 6000000
	defaultTimeoutMs              = 100
	defaultErrorsExitCount        = 0
)

const getEnvErrorLog = "failed to connect kafka: can't get %s param"

//todo: throw errors and handle them at upper level? it will change contract

type KafkaConnector struct {
	Logger log.FieldLogger
}

func NewKafkaConnector() *KafkaConnector {
	return &KafkaConnector{
		Logger: log.New(),
	}
}

func (k *KafkaConnector) SetLogger(logger log.FieldLogger) {
	k.Logger = logger
}

// Deprecated: GetConfigMap
func (k *KafkaConnector) GetConfigMap(forConsumer bool) *kafka.ConfigMap {
	configMap := k.getBaseMap()
	k.setSecurityConfigs(configMap)
	if forConsumer {
		k.setConsumerConfigs(configMap)
	}

	return configMap
}

func (k *KafkaConnector) GetConsumerConfigMap() *kafka.ConfigMap {
	configMap := k.getWithSecurityConfigMap()
	k.setConsumerConfigs(configMap)

	return configMap
}

func (k *KafkaConnector) GetProducerConfigMap() *kafka.ConfigMap {
	configMap := k.getWithSecurityConfigMap()

	return configMap
}

func (k *KafkaConnector) getBaseMap() *kafka.ConfigMap {
	KafkaUrl := env.GetStringOrDefault(kafkaUrlEnv, "")
	if KafkaUrl == "" {
		k.Logger.Fatalf(getEnvErrorLog, kafkaUrlEnv)
	}

	KafkaMaxMessageSize := env.GetIntOrDefault(kafkaMaxMessageSizeEnv, defaultKafkaMaxMessageSize)

	return &kafka.ConfigMap{
		"metadata.broker.list": KafkaUrl,
		"message.max.bytes":    KafkaMaxMessageSize,
	}
}

func (k *KafkaConnector) getWithSecurityConfigMap() *kafka.ConfigMap {
	configMap := k.getBaseMap()
	k.setSecurityConfigs(configMap)

	return configMap
}

func (k *KafkaConnector) setSecurityConfigs(configMap *kafka.ConfigMap) {
	KafkaUsername := env.GetStringOrDefault(kafkaUsernameEnv, "")
	KafkaPassword := env.GetStringOrDefault(kafkaPasswordEnv, "")

	if KafkaUsername != "" || KafkaPassword != "" {
		if KafkaUsername == "" {
			k.Logger.Fatalf(getEnvErrorLog, kafkaUsernameEnv)
		}
		if KafkaPassword == "" {
			k.Logger.Fatalf(getEnvErrorLog, kafkaPasswordEnv)
		}

		KafkaSecurityProtocol := env.GetStringOrDefault(kafkaSecurityProtocolEnv, "")
		if KafkaSecurityProtocol == "" {
			k.Logger.Fatalf(getEnvErrorLog, kafkaSecurityProtocolEnv)
		}

		KafkaSaslMechanism := env.GetStringOrDefault(kafkaSaslMechanismEnv, "")
		if KafkaSaslMechanism == "" {
			k.Logger.Fatalf(getEnvErrorLog, kafkaSaslMechanismEnv)
		}

		_ = configMap.SetKey("security.protocol", KafkaSecurityProtocol)
		_ = configMap.SetKey("sasl.username", KafkaUsername)
		_ = configMap.SetKey("sasl.password", KafkaPassword)
		_ = configMap.SetKey("sasl.mechanism", KafkaSaslMechanism)
	}
}

func (k *KafkaConnector) setConsumerConfigs(configMap *kafka.ConfigMap) {
	KafkaGroupId := env.GetStringOrDefault(kafkaGroupIdEnv, "")
	if KafkaGroupId == "" {
		k.Logger.Fatalf(getEnvErrorLog, kafkaGroupIdEnv)
	}

	KafkaAutoOffsetReset := env.GetStringOrDefault(kafkaAutoOffsetResetEnv, defaultKafkaAutoOffsetReset)
	KafkaMaxPollIntervalMs := env.GetIntOrDefault(kafkaMaxPollIntervalMsEnv, defaultKafkaMaxPollIntervalMs)

	_ = configMap.SetKey("auto.offset.reset", KafkaAutoOffsetReset)
	_ = configMap.SetKey("group.id", KafkaGroupId)
	_ = configMap.SetKey("max.poll.interval.ms", KafkaMaxPollIntervalMs)
}

func (k *KafkaConnector) GetPollTimeoutMs() int {
	timeoutMs := env.GetIntOrDefault(TimeoutMsEnv, defaultTimeoutMs)

	return timeoutMs
}

func (k *KafkaConnector) GetMaxErrorsExitCount() int {
	errorsExitCount := env.GetIntOrDefault(ErrorsExitCountEnv, defaultErrorsExitCount)

	return errorsExitCount
}
