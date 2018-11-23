package eventsourceconfig

import (
	"os"
	"strconv"
	"strings"
)

type eventsourceconfig struct {
	Target           string
	Host             string
	BootStrapServers string
	ConsumerGroupID  string
	KafkaTopic       string
	LogLevel         string
	LogFormat        string
	Saslconfig       saslconfig
}

type saslconfig struct {
	Enable    bool
	Handshake bool
	User      string
	Password  string
}

//GetConfig gets the initial config
func GetConfig() eventsourceconfig {

	saslEnable, _ := strconv.ParseBool(getEnv("SASL_ENABLE", "false"))
	saslHandshake, _ := strconv.ParseBool(getEnv("SASL_HANDSHAKE", "false"))

	return eventsourceconfig{
		BootStrapServers: strings.ToLower(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
		KafkaTopic:       os.Getenv("KAFKA_TOPIC"),
		ConsumerGroupID:  getEnv("CONSUMER_GROUP_ID", "consumerGroupID"),
		Target:           os.Getenv("TARGET"),
		LogLevel:         strings.ToLower(getEnv("LOG_LEVEL", "info")),
		LogFormat:        strings.ToLower(getEnv("LOG_FORMAT", "text")), //can be text or json
		Saslconfig: saslconfig{

			Enable:    saslEnable,
			Handshake: saslHandshake,
			User:      os.Getenv("SASL_USER"),
			Password:  os.Getenv("SASL_PASSWORD"),
		},
	}
}

func getEnv(key string, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
