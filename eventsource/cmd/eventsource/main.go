package main

import (
	"encoding/json"
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
	"github.com/rh-event-flow-incubator/KafkaEventSource/eventsource/pkg/eventsourceconfig"
)

func main() {

	eventsourceconfig := eventsourceconfig.GetConfig()
	log.Printf("BOOTSTRAP_SERVERS: %s", eventsourceconfig.BootStrapServers)

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	//SASL config
	config.Net.MaxOpenRequests = int(eventsourceconfig.NetMaxOpenRequests)
	config.Net.KeepAlive = time.Duration(eventsourceconfig.NetKeepAlive)
	config.Net.SASL.Enable = eventsourceconfig.NetSaslEnable
	config.Net.SASL.Handshake = eventsourceconfig.NetSaslHandshake
	config.Net.SASL.User = eventsourceconfig.NetSaslUser
	config.Net.SASL.Password = eventsourceconfig.NetSaslPassword
	config.Consumer.MaxWaitTime = time.Duration(eventsourceconfig.ConsumerMaxWaitTime)
	config.Consumer.MaxProcessingTime = time.Duration(eventsourceconfig.ConsumerMaxProcessingTime)
	config.Consumer.Offsets.CommitInterval = time.Duration(eventsourceconfig.ConsumerOffsetsCommitInterval)
	config.Consumer.Offsets.Retention = time.Duration(eventsourceconfig.ConsumerOffsetsRetention)
	config.Consumer.Offsets.Retry.Max = int(eventsourceconfig.ConsumerOffsetsRetrymax)
	config.ChannelBufferSize = int(eventsourceconfig.ChannelBufferSize)
	config.Group.Session.Timeout = time.Duration(eventsourceconfig.GroupSessionTimeout)

	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	if eventsourceconfig.ConsumerOffsetsInitial == "OffsetOldest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	config.Group.PartitionStrategy = cluster.StrategyRange
	if eventsourceconfig.GroupPartitionStrategy == "roundrobin" {
		config.Group.PartitionStrategy = cluster.StrategyRoundRobin
	}

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

				var raw map[string]interface{}
				err := json.Unmarshal(msg.Value, &raw)
				if err != nil {
					postMessage(eventsourceconfig.Target, msg.Value)
				} else {
					postMessage(eventsourceconfig.Target, raw)
				}

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

func postMessage(target string, value interface{}) error {
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
