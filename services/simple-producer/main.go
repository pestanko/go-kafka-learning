package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

type AppConfig struct {
	brokers []string
	topic   string
}

func main() {
	cfg := AppConfig{
		brokers: []string{"localhost:29092"},
		topic:   "simple-topic",
	}
	fmt.Printf("Trying to attach to kafka brokers: %v\n", cfg.brokers)
	config := makeSaramaConfig()
	producer, err := sarama.NewSyncProducer(cfg.brokers, config)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		sendMessage(&cfg, producer, i)
		time.Sleep(time.Millisecond * 200)
	}
}

func sendMessage(cfg *AppConfig, producer sarama.SyncProducer, id int) {
	msg := fmt.Sprintf("%v: Hello world!", id)
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
	log.Printf("Partition: %v; Offset: %v\n", partition, offset)
}

func makeSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	return config
}
