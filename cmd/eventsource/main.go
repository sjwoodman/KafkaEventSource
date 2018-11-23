package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/google/uuid"
	"github.com/knative/pkg/cloudevents"
	"github.com/rh-event-flow-incubator/KafkaEventSource/pkg/eventsourceconfig"
)

func main() {

	eventsourceconfig := eventsourceconfig.GetConfig()
	log.Printf("BOOTSTRAP_SERVERS: %s", eventsourceconfig.BootStrapServers)

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	//SASL config
	config.Net.SASL.Enable = eventsourceconfig.Saslconfig.Enable
	config.Net.SASL.Handshake = eventsourceconfig.Saslconfig.Handshake
	config.Net.SASL.User = eventsourceconfig.Saslconfig.User
	config.Net.SASL.Password = eventsourceconfig.Saslconfig.Password

	//todo
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// init consumer
	brokers := []string{eventsourceconfig.BootStrapServers}
	topics := []string{eventsourceconfig.KafkaTopic}
	consumerGroupID := eventsourceconfig.ConsumerGroupID

	consumer, err := cluster.NewConsumer(brokers, consumerGroupID, topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				log.Printf("Received %s", msg.Value)

				postMessage(eventsourceconfig.Target, msg.Value)

				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			return
		}
	}
}

// Creates a CloudEvent Context for a given Kafka ConsumerMessage.
func cloudEventsContext() *cloudevents.EventContext {
	return &cloudevents.EventContext{
		// Events are themselves object and have a unique UUID. Could also have used the UID
		CloudEventsVersion: cloudevents.CloudEventsVersion,
		EventType:          "dev.knative.k8s.event",
		EventID:            string(uuid.New().String()),
		Source:             "kafka-demo",
		EventTime:          time.Now(),
	}
}

func postMessage(target string, value []byte) error {
	ctx := cloudEventsContext()

	log.Printf("Posting to %q", target)
	// Explicitly using Binary encoding so that Istio, et. al. can better inspect
	// event metadata.
	req, err := cloudevents.Binary.NewRequest(target, value, *ctx)
	if err != nil {
		log.Printf("Failed to create http request: %s", err)
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
