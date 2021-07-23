package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Интерфейс работы с сообщениями
type Message interface {
	// Обработать сообщение
	Handle(msg kafka.Message)
}
