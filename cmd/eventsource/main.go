package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/knative/pkg/cloudevents"
	"github.com/rh-event-flow-incubator/KafkaEventSource/pkg/config"
)

func main() {

	//Setup the input
	config := config.GetConfig()

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

			postMessage(config.Target, msg.Value)

		case <-signals:
			break ConsumerLoop
		}
	}
}

// Creates a CloudEvent Context for a given Kafka ConsumerMessage.
func cloudEventsContext() *cloudevents.EventContext {
	return &cloudevents.EventContext{
		CloudEventsVersion: cloudevents.CloudEventsVersion,
		EventType:          "dev.knative.source.kafka",
		EventID:            uuid.New().String(),
		Source:             "kafka-demo",
		EventTime:          time.Now(),
	}
}

func postMessage(target string, value []byte) error {

	ctx := cloudEventsContext()

	log.Printf("posting to %q", target)
	// Explicitly using Binary encoding so that Istio, et. al. can better inspect
	// event metadata.
	req, err := cloudevents.Binary.NewRequest(target, value, *ctx)
	if err != nil {
		log.Printf("failed to create http request: %s", err)
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to do POST: %v", err)
		return err
	}
	defer resp.Body.Close()
	log.Printf("response Status: %s", resp.Status)
	body, _ := ioutil.ReadAll(resp.Body)
	log.Printf("response Body: %s", string(body))
	return nil

}
