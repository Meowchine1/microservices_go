package main

import (
	"config"
	"database/sql"
	"fmt"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func main() {
	// Подключение к RabbitMQ
	conn, err := amqp091.Dial(config.RabbitMQURL)
	config.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	config.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	connectionString := "sqlserver://sa:sa123@localhost:1433?database=delivery_service&connectiontimeout=30&maxconns=100"
	//connectionString := "delivery_service@localhost"
	db, err := sql.Open("sqlserver", connectionString)
	config.FailOnError(err, "Failed to connect to database")
	defer db.Close()

	// Объявление очереди для отправки уведомлений об обработке заказа (передача в доставку)
	qDeliveredOrders, err := config.DeclareQueue(ch, config.DeliveryQueueName)
	// Объявление очереди для получения уведомлений о создании заказа
	qAgreedOrders, err := config.DeclareQueue(ch, config.OrderQueueName)

	// Канал для завершения программы
	forever := make(chan struct{})

	// Горутина для прослушивания очереди "delivery"
	go func() {
		msgs, err := ch.Consume(
			qAgreedOrders.Name, // очередь
			"",                 // consumer
			true,               // auto-ack
			false,              // exclusive
			false,              // no-local
			false,              // no-wait
			nil,                // args
		)
		config.FailOnError(err, "Failed to register a consumer for 'delivery' queue")

		// Горутина для обработки полученных сообщений
		for d := range msgs {
			log.Printf("Received delivery notification: %s", d.Body)

			event := config.InboxEvent{
				ServiceName: "UserService",
				EventType:   "OrderDelivered",
				Payload:     fmt.Sprintf("Order %s delivered", d.Body),
				CreatedAt:   time.Now(),
			}
			err := config.InsertInboxEvent(db, event)
			config.FailOnError(err, "Failed to insert event into inbox")

			log.Printf("Order %s has been delivered. User notified.", d.Body)
		}
	}()

	// Горутина для отправки заказов раз в 5 секунд
	go func() {
		for {
			// Создание события заказа
			event := config.OutboxEvent{
				ServiceName: "DeliveryService",
				EventType:   "Order delivered",
				Payload:     fmt.Sprintf("New order delivered"),
				CreatedAt:   time.Now(),
			}

			// Вставка события в таблицу Outbox
			err = config.InsertOutboxEvent(db, event)
			config.FailOnError(err, "Failed to insert event into outbox")

			// Отправка события о доставке заказа в очередь RabbitMQ
			err = ch.Publish(
				"",                    // exchange
				qDeliveredOrders.Name, // routing key
				false,                 // mandatory
				false,                 // immediate
				amqp091.Publishing{
					ContentType: "text/plain",
					Body:        []byte(event.Payload),
				},
			)
			config.FailOnError(err, "Failed to publish a message to 'delivery_queue' queue")
			log.Printf(" [x] Order %d created and sent to 'delivery_queue' queue", event.ID)

			// Задержка 5 секунд
			time.Sleep(5 * time.Second)
		}
	}()

	log.Printf(" [*] Waiting for delivery notifications and creating orders. To exit press CTRL+C")

	// Ожидаем завершения программы
	<-forever
}
