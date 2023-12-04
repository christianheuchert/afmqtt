package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/support/ssl"
	"github.com/project-flogo/core/trigger"
)

var triggerMd = trigger.NewMetadata(&Output{})

// Global config
var config = readConfig("mqtt.json") // config data

func init() {
	_ = trigger.Register(&Trigger{}, &Factory{})
}

// TokenType is a type of token
type TokenType int

const (
	// Literal is a literal token type
	Literal TokenType = iota
	// SingleLevel is a single level wildcard
	SingleLevel
	// MultiLevel is a multi level wildcard
	MultiLevel
)

// Token is a MQTT topic token
type Token struct {
	TokenType TokenType
	Token     string
}

// Topic is a parsed topic
type Topic []Token

// ParseTopic parses the topic
func ParseTopic(topic string) Topic {
	var parsed Topic
	parts, index := strings.Split(topic, "/"), 0
	for _, part := range parts {
		if strings.HasPrefix(part, "+") {
			token := strings.TrimPrefix(part, "+")
			if token == "" {
				token = strconv.Itoa(index)
				index++
			}
			parsed = append(parsed, Token{
				TokenType: SingleLevel,
				Token:     token,
			})
		} else if strings.HasPrefix(part, "#") {
			token := strings.TrimPrefix(part, "#")
			if token == "" {
				token = strconv.Itoa(index)
				index++
			}
			parsed = append(parsed, Token{
				TokenType: MultiLevel,
				Token:     token,
			})
		} else {
			parsed = append(parsed, Token{
				TokenType: Literal,
				Token:     part,
			})
		}
	}
	return parsed
}

// Match matches the topic with an input topic
func (t Topic) Match(input Topic) map[string]string {
	output, i := make(map[string]string), 0
MATCHER:
	for _, token := range t {
		switch token.TokenType {
		case Literal:
			if i > len(input) {
				break MATCHER
			}
			if token.Token != input[i].Token {
				break MATCHER
			}
		case SingleLevel:
			if i >= len(input) {
				output[token.Token] = ""
				break MATCHER
			}
			output[token.Token] = input[i].Token
		case MultiLevel:
			if i >= len(input) {
				output[token.Token] = ""
				break MATCHER
			}
			levels, sep := "", ""
			for _, level := range input[i:] {
				levels += sep + level.Token
				sep = "/"
				i++
			}
			output[token.Token] = levels
			break MATCHER
		}
		i++
	}
	return output
}

// String generates a string for the topic
func (t Topic) String() string {
	output := strings.Builder{}
	for i, token := range t {
		if i > 0 {
			output.WriteString("/")
		}
		switch token.TokenType {
		case Literal:
			output.WriteString(token.Token)
		case SingleLevel:
			output.WriteString("+")
		case MultiLevel:
			output.WriteString("#")
		}
	}
	return output.String()
}

// Trigger is simple MQTT trigger
type Trigger struct {
	handlers map[string]*clientHandler
	logger   log.Logger
	options  *mqtt.ClientOptions
	client   mqtt.Client
}
type clientHandler struct {
	//client mqtt.Client
	handler  trigger.Handler
}
type Factory struct {
}

func (*Factory) Metadata() *trigger.Metadata {
	return triggerMd
}

// New implements trigger.Factory.New
func (*Factory) New(config *trigger.Config) (trigger.Trigger, error) {
	return &Trigger{}, nil
}

// Initialize implements trigger.Initializable.Initialize
func (t *Trigger) Initialize(ctx trigger.InitContext) error {
	t.logger = ctx.Logger()

	options := initClientOption()
	t.options = options

	if strings.HasPrefix(config.BrokerURL, "ssl") {

		cfg := &ssl.Config{}

		{
			//using ssl but not configured, use defaults
			cfg.SkipVerify = true
			cfg.UseSystemCert = true
		}

		tlsConfig, err := ssl.NewClientTLSConfig(cfg)
		if err != nil {
			return err
		}

		options.SetTLSConfig(tlsConfig)
	}

	options.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		t.logger.Warnf("Recieved message on unhandled topic: %s", msg.Topic())
	})

	t.logger.Debugf("Client options: %v", options)

	t.handlers = make(map[string]*clientHandler)

	return nil
}

func initClientOption() *mqtt.ClientOptions {

	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.BrokerURL)
	opts.SetClientID(config.BrokerId)
	opts.SetUsername(config.BrokerUsername)
	opts.SetPassword(config.BrokerPassword)

	{
		opts.SetKeepAlive(2 * time.Second)
	}

	return opts
}

// Start implements trigger.Trigger.Start
func (t *Trigger) Start() error {

	client := mqtt.NewClient(t.options)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	t.client = client

	for _, handler := range t.handlers {
		// parsed := ParseTopic(handler.settings.Topic)
		parsed := ParseTopic(config.BrokerTopic) // NEW
		if token := client.Subscribe(parsed.String(), 0, t.getHanlder(handler, parsed)); token.Wait() && token.Error() != nil {
			t.logger.Errorf("Error subscribing to topic %s: %s", parsed, token.Error())
			return token.Error()
		}

		t.logger.Debugf("Subscribed to topic: %s", parsed)
	}

	return nil
}

// Stop implements ext.Trigger.Stop
func (t *Trigger) Stop() error {

	//unsubscribe from topics
	for _, _ = range t.handlers {
		topic := config.BrokerTopic
		t.logger.Debug("Unsubscribing from topic: ", topic)
		if token := t.client.Unsubscribe(topic); token.Wait() && token.Error() != nil {
			t.logger.Errorf("Error unsubscribing from topic %s: %s", topic, token.Error())
		}
	}

	t.client.Disconnect(250)

	return nil
}

func (t *Trigger) getHanlder(handler *clientHandler, parsed Topic) func(mqtt.Client, mqtt.Message) {
	return func(client mqtt.Client, msg mqtt.Message) {
		topic := msg.Topic()
		payload := string(msg.Payload())

		t.logger.Debugf("Topic[%s] - Payload Recieved: %s", topic, payload)
	}
}

// RunHandler runs the handler and associated action
func runHandler(handler trigger.Handler, payload, topic string, params map[string]string) (map[string]interface{}, error) {
	out := &Output{
		Message:     payload,
		Topic:       topic,
		TopicParams: params,
	}

	results, err := handler.Handle(context.Background(), out)
	if err != nil {
		return nil, err
	}

	return results, nil
}

// 	config := readConfig("mqtt.json") // config data
func readConfig(configPath string) Config {
	var config Config

	// Read the JSON file.
    jsonFile, err := os.ReadFile(configPath)
    if err != nil {
        fmt.Println(err)
        return config
    }

    // Decode the JSON file into the config struct.
    err = json.Unmarshal(jsonFile, &config)
    if err != nil {
        fmt.Println(err)
        return config
    }

	config.BrokerId = generateUniqueID()

	return config
}
func generateUniqueID() string {
    baseID := "AFmqtt"
    timestamp := time.Now().UnixNano()
    return fmt.Sprintf("%s_%d", baseID, timestamp)
}

type Config struct {
	ConfigFolderPath string
    BrokerURL string
    BrokerPassword string
    BrokerUsername string
	BrokerId string
	BrokerTopic string
}