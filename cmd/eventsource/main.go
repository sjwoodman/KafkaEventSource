package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/matzew/kafka-receiver/pkg/config"
)

func main() {

	//Setup the input
	config := config.GetConfig()
	config.BootStrapServers = os.Getenv("BOOTSTRAP_SERVERS")
	config.KafkaTopic = os.Getenv("TOPIC")

	log.Printf("BOOTSTRAP_SERVERS: %s", config.BootStrapServers)

	consumer, err := sarama.NewConsumer([]string{config.BootStrapServers}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	initialOffset := sarama.OffsetNewest //only deal with new messages
	partitionConsumer, err := consumer.ConsumePartition(config.KafkaTopic, 0, initialOffset)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Printf("Connected to %s", config.KafkaTopic)

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():

			//Call the Serving function
			req, err := http.NewRequest("POST", os.Getenv("TARGET"), bytes.NewBuffer(msg.Value))
			req.Host = os.Getenv("HOST")

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("Error sending to Serving function: %s", err)
			}

			defer resp.Body.Close()

			fmt.Println("response Status:", resp.Status)
			fmt.Println("response Headers:", resp.Header)

		case <-signals:
			break ConsumerLoop
		}
	}
}
