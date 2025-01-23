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

	// Подключение к базе данных (PostgreSQL)
	connectionString := "sqlserver://sa:sa123@localhost:1433?database=order_service&connectiontimeout=30&maxconns=100"
	//connectionString := "order_service@localhost"
	db, err := sql.Open("sqlserver", connectionString)
	config.FailOnError(err, "Failed to connect to database")
	defer db.Close()

	// Объявление очереди для отправки уведомлений об обработке заказа (передача в доставку)
	qAgreeOrders, err := config.DeclareQueue(ch, config.OrderQueueName)
	config.FailOnError(err, "Failed to declare 'order_queue'")
	// Объявление очереди для получения уведомлений о создании заказа
	qCreatedOrders, err := config.DeclareQueue(ch, config.OrderCreateQueueName)
	config.FailOnError(err, "Failed to declare 'order_create_queue'")

	orderID := 0 // Индекс заказов
	// Канал для завершения программы
	forever := make(chan struct{})

	// Горутина для прослушивания очереди
	go func() {
		msgs, err := ch.Consume(
			qCreatedOrders.Name, // очередь
			"",                  // consumer
			true,                // auto-ack
			false,               // exclusive
			false,               // no-local
			false,               // no-wait
			nil,                 // args
		)
		config.FailOnError(err, "Failed to register a consumer for 'created_orders' queue")

		// Горутина для обработки полученных сообщений
		for d := range msgs {
			log.Printf("Received delivery notification: %s", d.Body)

			// Вставка события в таблицу Outbox
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
				ServiceName: "OrderService",
				EventType:   "Order agreed",
				Payload:     fmt.Sprintf("New order agreed"),
				CreatedAt:   time.Now(),
			}

			// Вставка события в таблицу Outbox
			err = config.InsertOutboxEvent(db, event)
			config.FailOnError(err, "Failed to insert event into outbox")

			// Отправка события о создании заказа в очередь RabbitMQ
			err = ch.Publish(
				"",                // exchange
				qAgreeOrders.Name, // routing key
				false,             // mandatory
				false,             // immediate
				amqp091.Publishing{
					ContentType: "text/plain",
					Body:        []byte(event.Payload),
				},
			)
			config.FailOnError(err, "Failed to publish a message to 'order_queue' queue")
			log.Printf(" [x] Order %d created and sent to 'order_queue' queue", event.ID)

			// Задержка 5 секунд
			orderID++
			time.Sleep(5 * time.Second)
		}
	}()

	log.Printf(" [*] Waiting for delivery notifications and creating orders. To exit press CTRL+C")

	// Ожидаем завершения программы
	<-forever
}
