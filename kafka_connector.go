package connector

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"os"
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

	configMap := kafka.ConfigMap{
		"metadata.broker.list": KafkaUrl,
		"security.protocol":    KafkaSecurityProtocol,
		"sasl.username":        KafkaUsername,
		"sasl.password":        KafkaPassword,
		"sasl.mechanism":       KafkaSaslMechanism,
		"group.id":             KafkaGroupId,
		"auto.offset.reset":    "earliest",
		//"enable.auto.commit":   false,
	}
	return &configMap
}
