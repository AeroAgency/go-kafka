package connector

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

type KafkaConnector struct {
}

func NewKafkaConnector() *KafkaConnector {
	return &KafkaConnector{}
}
func (k KafkaConnector) GetConfigMap() *kafka.ConfigMap {
	KafkaUrl, ok := os.LookupEnv("KAFKA_URL")
	if !ok {
		log.Fatalf("failed to connect kafka: can't get KAFKA_URL param")
	}
	KafkaSecurityProtocol, ok := os.LookupEnv("KAFKA_SECURITY_PROTOCOL")
	if !ok {
		log.Fatalf("failed to connect kafka: can't get KAFKA_SECURITY_PROTOCOL param")
	}
	KafkaUsername, ok := os.LookupEnv("KAFKA_USER_NAME")
	if !ok {
		log.Fatalf("failed to connect kafka: can't get KAFKA_USER_NAME param")
	}
	KafkaPassword, ok := os.LookupEnv("KAFKA_PASSWORD")
	if !ok {
		log.Fatalf("failed to connect kafka: can't get KAFKA_PASSWORD param")
	}
	KafkaSaslMechanism, ok := os.LookupEnv("KAFKA_SASL_MECHANISM")
	if !ok {
		log.Fatalf("failed to connect kafka: can't get KAFKA_SASL_MECHANISM param")
	}
	KafkaGroupId, ok := os.LookupEnv("KAFKA_GROUP_ID")
	if !ok {
		log.Fatalf("failed to connect kafka: can't get KAFKA_GROUP_ID param")
	}
	KafkaMaxMessageSize := 1048576
	EnvKafkaMaxMessageSize, ok := os.LookupEnv("KAFKA_MESSAGE_MAX_BYTES")
	if ok {
		parseSize, err := strconv.Atoi(EnvKafkaMaxMessageSize)
		if err == nil {
			KafkaMaxMessageSize = parseSize
		}
	}

	configMap := kafka.ConfigMap{
		"metadata.broker.list": KafkaUrl,
		"security.protocol":    KafkaSecurityProtocol,
		"sasl.username":        KafkaUsername,
		"sasl.password":        KafkaPassword,
		"sasl.mechanism":       KafkaSaslMechanism,
		"group.id":             KafkaGroupId,
		"auto.offset.reset":    "earliest",
		"message.max.bytes":    KafkaMaxMessageSize,
		"max.poll.interval.ms": 6000000,
		//"enable.auto.commit":   false,
	}
	return &configMap
}
