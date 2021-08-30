//Напишите небольшую программу, состоящую только из исполняемого пакета,
//которая бы в одном потоке отправляла в Kafka сообщения,
//а в другом потоке читала бы их и выводила значение (Value) на экран.
//Сообщения должны отправляться в очередь раз в секунду
//и содержать в качестве значения текущее представление времени в виде строки.

//Программа должна использовать вариант с
//отдельными операциями выборки и подтверждения чтения сообщения.

package main

import (
	"errors"

	"github.com/segmentio/kafka-go"
)

type clientLocal struct {
	Reader *kafka.Reader
	Writer *kafka.Writer
}

func New(brokers []string, topic string) (*clientLocal, error) {
	if len(brokers) == 0 || brokers[0] == "" || topic == "" {
		return nil, errors.New("client params is not enough")
	}
	c := clientLocal{}

	c.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic})

	c.Writer = &kafka.Writer{
		Addr:  kafka.TCP(brokers[0]),
		Topic: topic,
	}
	return &c, nil
}

func main() {

}
