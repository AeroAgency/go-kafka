# Оглавление

### Назначение:

Сервис для работы с kafka

## При использовании добавить и заполнить следующие параметры окружения
KAFKA_URL=kafka.ru:30001 

KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT 

KAFKA_USER_NAME=username 

KAFKA_PASSWORD=pass 

KAFKA_SASL_MECHANISM=SCRAM-SHA-256 

KAFKA_GROUP_ID=consumerName 

KAFKA_TOPIC=consumerTopic


## Пример использования консюмера

* Имплементировать интерфейс consumer.Message Реализовать в нем обработку входящего сообщения
* Собирать контейнер для Sap Consumer Пример:
  `func buildSapConsumerContainer(ctn di.Container) (interface{}, error) { importService := ctn.Get("importService").(*service.ImportService)
  message := message.NewMessageHandler(importService)
  sapConsumer := consumer.NewSapConsumer(message)
  return sapConsumer, nil }`

* Вызвать StartConsumer
  `ctn, _ := registry.NewContainer()
  consumer := ctn.Resolve("sap_consumer").(*consumer.SapConsumer)
  consumer.KafkaConsumer.StartConsumer()`