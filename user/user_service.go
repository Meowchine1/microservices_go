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
	conn, err := amqp091.Dial(config.RabbitMQURL)
	config.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	config.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	connectionString := "sqlserver://sa:sa123@localhost:1433?database=user_service&connectiontimeout=30&maxconns=100"
	//connectionString := "user_service@localhost"
	db, err := sql.Open("sqlserver", connectionString)
	config.FailOnError(err, "Failed to connect to database")
	defer db.Close()

	// очередь для отправки уведомлений о создании заказа
	qCreateOrders, err := config.DeclareQueue(ch, config.OrderCreateQueueName)
	config.FailOnError(err, "Failed to declare 'order_create_queue'")

	// очередь для получения уведомлений о доставке
	qDelivery, err := config.DeclareQueue(ch, config.DeliveryQueueName)
	config.FailOnError(err, "Failed to declare 'delivery_queue'")

	// канал для завершения программы
	forever := make(chan struct{})

	// прослушивание очереди "delivery"
	go func() {
		msgs, err := ch.Consume(
			qDelivery.Name, // очередь
			"",             // consumer
			true,           // auto-ack
			false,          // exclusive
			false,          // no-local
			false,          // no-wait
			nil,            // args
		)
		config.FailOnError(err, "Failed to register a consumer for 'delivery' queue")

		for d := range msgs {
			log.Printf("Received delivery notification: %s", d.Body)

			// Вставка события в таблицу Inbox
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

	// отправки заказов раз в 5 секунд
	go func() {
		for {
			// Создание события заказа
			event := config.OutboxEvent{
				ServiceName: "UserService",
				EventType:   "OrderCreated",
				Payload:     fmt.Sprintf("New order  created"),
				CreatedAt:   time.Now(),
			}

			// Вставка события в таблицу Outbox
			err := config.InsertOutboxEvent(db, event)
			config.FailOnError(err, "Failed to insert event into outbox")

			// Отправка события о создании заказа в очередь RabbitMQ
			err = ch.Publish(
				"",                 // exchange
				qCreateOrders.Name, // routing key
				false,              // mandatory
				false,              // immediate
				amqp091.Publishing{
					ContentType: "text/plain",
					Body:        []byte(event.Payload),
				},
			)
			config.FailOnError(err, "Failed to publish a message to 'order_create_queue'")
			log.Printf(" [x] Order %d created and sent to 'order_create_queue'", event.ID)

			// Задержка 5 секунд

			time.Sleep(5 * time.Second)
		}
	}()

	log.Printf(" [*] Waiting for delivery notifications and creating orders. To exit press CTRL+C")

	// Ожидание завершения программы
	<-forever
}
