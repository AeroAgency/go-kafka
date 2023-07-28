# Оглавление

### Назначение:

Пакет для работы с kafka

### Конфигурация с использованием переменных окружения

[Пример файла .env](.env.example)

| Имя                         | Описание                                                        | Обязательное | Значение по-умолчанию |
|-----------------------------|-----------------------------------------------------------------|--------------|-----------------------|
| KAFKA_URL                   | Адрес брокера                                                   | Да           | –                     |
| KAFKA_SECURITY_PROTOCOL     | Метод аутентификации                                            | Да*          | –                     |
| KAFKA_SASL_MECHANISM        | SASL механизм                                                   | Да*          | –                     |
| KAFKA_USER_NAME             | Имя пользователя                                                | Да*          | –                     |
| KAFKA_PASSWORD              | Пароль                                                          | Да*          | –                     |
| KAFKA_GROUP_ID              | Id Консьюмер группы                                             | Да**         | –                     |
| KAFKA_MESSAGE_MAX_BYTES     | Макисмальный размер сообщения (байт)                            | Нет          | 1048576               |
| KAFKA_AUTO_OFFSET_RESET     | Консьюмер начинает читать с начала или с конца партиции         | Нет          | earliest              |
| KAFKA_MAX_POLL_INTERVAL_MS  | Максимальная задержка между вызовами poll консьюмера (мс)       | Нет          | 6000000               |
| KAFKA_POLL_TIMEOUT_MS       | Таймаут метода poll консьюмера (мс)                             | Нет          | 100                   |
| KAFKA_MAX_ERRORS_EXIT_COUNT | Максимальное количество полученных подряд ошибок консьюмером*** | Нет          | 0***                  |

*Если требуется аутентификация

**Для консьюмера

***При достижении этого порога консьюмер прекратит работу, при чтении сообщения счётчик сбрасывается, 0 означает неограниченное кол-во ошибок

### Конфигурация с помощью интерфейса
Для конфигрурации также можно реализовать интерфейс `KafkaConnector`

## Примеры использования
### Использование консьюмера

* Для обработки входящих сообщений необходимо имплементировать интерфейс Message

    ```go
    package handler
  
    type Handler struct {
        //...
    }
  
    func NewHandler() *Handler {
        return &Handler{}
    }
  
    func (h *Handler) Handle(msg kafka.Message) {
        //...
    }
    ```
  
* Создание консьюмера

    ```go
    package consumer
    import "github.com/AeroAgency/go-kafka/consumer"
  
    type SomeConsumer struct {
		*consumer.KafkaConsumer
    }
  
    func NewSomeConsumer(message consumer.Message) *SomeConsumer {
        return &SomeConsumer{
            consumer.NewKafkaConsumer(message),
        }
    }
    ```

    ```go
    package main
    import (
    	"consumer"
        "handler"
    )
  
    func main(){
        message := handler.NewHandler()
        someConsumer := consumer.NewSomeConsumer(message)
        someConsumer.StartConsumer("someTopic")
    }
    ```

* Использование совместо с sarulabs/di
  ```go
  package container
  
  import ( 
    "github.com/sarulabs/di/v2"
  )
  
  type Container struct {
    сtn di.Container
  }
  
  func NewContainer(ctx context.Context) (*Container, error) {
    builder, err := di.NewBuilder()
    if err != nil {
      return nil, err
    }
  
    // ...
    
    return &Container{
      ctn: builder.Build(),
    }, nil
  }
  
  func buildConsumer(ctn di.Container) (interface{}, error) {
    someService := ctn.Get("someService").(*service.SomeService)
    handler := handler.NewHandler(someService)
    consumer := consumer.NewConsumer(handler)
    return consumer, nil 
  }
  ```

  ```go
  package main
  
  func main() {
    ctn, _ := registry.NewContainer()
    consumer := ctn.Resolve("consumer").(*consumer.Consumer)
    consumer.KafkaConsumer.StartConsumer("topic_name")
  }
  ```

### Использование продюсера
* Пример использования внутри сервиса
  ```go
  package service
  import "github.com/AeroAgency/go-kafka/producer"

  
  type Service struct {
    producer *producer.KafkaProducer
  }
  
  func NewService() *SomeService {
    return &SomeService{
      producer: producer.NewKafkaProducer(),
    }
  }
  
  func (s *Service) Process()  {
    err := s.producer.SendMessage("MyTopic", "someMessage")
    if err != nil {
      // handle error
      return 
    }
  }
  ```