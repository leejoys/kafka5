//Напишите небольшую программу, состоящую только из исполняемого пакета,
//которая бы в одном потоке отправляла в Kafka сообщения,
//а в другом потоке читала бы их и выводила значение (Value) на экран.
//Сообщения должны отправляться в очередь раз в секунду
//и содержать в качестве значения текущее представление времени в виде строки.

//Программа должна использовать вариант с
//отдельными операциями выборки и подтверждения чтения сообщения.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type clientLocal struct {
	Reader *kafka.Reader
	Writer *kafka.Writer
}

func New(brokers []string, topic string, groupID string) (*clientLocal, error) {
	if len(brokers) == 0 || brokers[0] == "" || topic == "" || groupID == "" {
		return nil, errors.New("client params is not enough")
	}
	c := clientLocal{}

	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})

	c.Writer = &kafka.Writer{
		Addr:  kafka.TCP(brokers[0]),
		Topic: topic,
	}
	return &c, nil
}

func (c clientLocal) sendMsgs(msg kafka.Message) error {
	err := c.Writer.WriteMessages(context.Background(), msg)
	return err
}

func (c clientLocal) getMsgs() (*kafka.Message, error) {
	msg, err := c.Reader.FetchMessage(context.Background())
	if err != nil {
		return nil, err
	}
	err = c.Reader.CommitMessages(context.Background(), msg)
	return &msg, err
}

func main() {
	kfk, err := New([]string{"localhost:9093"}, "test", "test-group")
	if err != nil {
		log.Fatalf("Client constructor error: %s", err)
	}

	go func() {
		for {
			var msg kafka.Message
			msg.Value = []byte(time.Now().String())
			err := kfk.sendMsgs(msg)
			if err != nil {
				log.Fatalf("Writer error: %s", err)
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for {
			msg, err := kfk.getMsgs()
			if err != nil {
				log.Fatalf("Reader error: %s", err)
			}
			fmt.Println(string(msg.Value))
			time.Sleep(time.Second)
		}
	}()
	time.Sleep(time.Second * 20)
}
