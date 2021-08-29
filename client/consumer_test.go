package client

import (
	"os"
	"testing"
)

func TestClient_getMessage(t *testing.T) {
	// Инициализация клиента Kafka.
	kfk, err := New(
		[]string{os.Getenv("KAFKA_BROKER")},
		"test",
		"test-consumer-group",
	)
	if err != nil {
		t.Fatal(err)
	}

	// получение следующего сообщения.
	msg, err := kfk.getMessage()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v\n%s\n%s", msg, string(msg.Key), string(msg.Value))
}
