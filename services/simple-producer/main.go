package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type AppConfig struct {
	brokers []string
	topic   string
}

func main() {
	cfg := AppConfig{
		brokers: []string{"localhost:29202"},
		topic:   "simple-topic",
	}
	fmt.Printf("Trying to attach to kafka brokers: %v\n", cfg.brokers)
	config := makeSaramaConfig()
	producer, err := sarama.NewSyncProducer(cfg.brokers, config)
	if err != nil {
		panic(err)
	}

	msg := "Hello world!"
	var partition int32 = -1
	message := &sarama.ProducerMessage{
		Topic:     cfg.topic,
		Partition: partition,
		Value:     sarama.StringEncoder(msg),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Partition: %v; Offset: %v\n", partition, offset)
}

func makeSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	return config
}
