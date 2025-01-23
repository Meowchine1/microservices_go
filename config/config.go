package config

import (
	"database/sql"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// Константы для RabbitMQ
const (
	RabbitMQURL          = "amqp://guest:guest@localhost:5672/"
	OrderCreateQueueName = "order_create_queue"
	OrderQueueName       = "order_queue"
	DeliveryQueueName    = "delivery_queue"

	SQL_delivery_service = "delivery_service"
	SQL_user_service     = "user_service"
	SQL_order_service    = "order_service"
)

// OutboxEvent представляет событие, которое отправляется в очередь
type OutboxEvent struct {
	ID          int
	ServiceName string
	EventType   string
	Payload     string
	CreatedAt   time.Time
}

// InboxEvent представляет событие, которое принимается из очереди
type InboxEvent struct {
	ID          int
	ServiceName string
	EventType   string
	Payload     string
	CreatedAt   time.Time
}

func InsertOutboxEvent(db *sql.DB, event OutboxEvent) error {

	query := `INSERT INTO Outbox (ServiceName, EventType, Payload, CreatedAt) 
              OUTPUT INSERTED.id 
              VALUES (@ServiceName, @EventType, @Payload, @CreatedAt)`

	// Выполнение запроса с параметрами
	err := db.QueryRow(query,
		sql.Named("ServiceName", event.ServiceName),
		sql.Named("EventType", event.EventType),
		sql.Named("Payload", event.Payload),
		sql.Named("CreatedAt", event.CreatedAt)).Scan(&event.ID)
	//	err := db.QueryRow(query, event.ServiceName, event.EventType, event.Payload, event.CreatedAt).Scan(&event.ID)
	if err != nil {
		log.Printf("Failed to insert outbox event: %s", err)
		return err
	}
	return nil
}

func InsertInboxEvent(db *sql.DB, event InboxEvent) error {
	query := `INSERT INTO Inbox (ServiceName, EventType, Payload, CreatedAt) 
              OUTPUT INSERTED.id 
              VALUES (@ServiceName, @EventType, @Payload, @CreatedAt)`

	// Выполнение запроса с параметрами
	err := db.QueryRow(query,
		sql.Named("ServiceName", event.ServiceName),
		sql.Named("EventType", event.EventType),
		sql.Named("Payload", event.Payload),
		sql.Named("CreatedAt", event.CreatedAt)).Scan(&event.ID)

	if err != nil {
		log.Printf("Failed to insert inbox event: %s", err)
		return err
	}
	return nil
}

// DeclareQueue объявляет очередь в RabbitMQ
func DeclareQueue(ch *amqp091.Channel, queueName string) (amqp091.Queue, error) {
	q, err := ch.QueueDeclare(
		queueName, // имя очереди
		false,     // durable (не будет сохраняться после перезапуска RabbitMQ)
		false,     // delete when unused
		false,     // exclusive (доступна только для этого соединения)
		false,     // no-wait
		nil,       // дополнительные аргументы
	)
	if err != nil {
		log.Printf("Failed to declare queue %s: %s", queueName, err)
		return q, err
	}
	log.Printf("Queue %s declared", queueName)
	return q, nil
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
