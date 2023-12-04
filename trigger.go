package mqtt

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// log is the default package logger
var log = logger.GetLogger("trigger-flogo-mqtt")

// MqttTrigger is simple MQTT trigger
type MqttTrigger struct {
	metadata       *trigger.Metadata
	client         mqtt.Client
	config         *trigger.Config
	handlers       []*trigger.Handler
	topicToHandler map[string]*trigger.Handler
}

//NewFactory create a new Trigger factory
func NewFactory(md *trigger.Metadata) trigger.Factory {
	return &MQTTFactory{metadata: md}
}

// MQTTFactory MQTT Trigger factory
type MQTTFactory struct {
	metadata *trigger.Metadata
}

//New Creates a new trigger instance for a given id
func (t *MQTTFactory) New(config *trigger.Config) trigger.Trigger {
	return &MqttTrigger{metadata: t.metadata, config: config}
}

// Metadata implements trigger.Trigger.Metadata
func (t *MqttTrigger) Metadata() *trigger.Metadata {
	return t.metadata
}

// Initialize implements trigger.Initializable.Initialize
func (t *MqttTrigger) Initialize(ctx trigger.InitContext) error {
	t.handlers = ctx.GetHandlers()
	return nil
}

// Start implements trigger.Trigger.Start
func (t *MqttTrigger) Start() error {

	config := readConfig("mqtt.json") // config data
	opts := mqtt.NewClientOptions()
	opts.AddBroker(config.BrokerURL)
	opts.SetClientID(config.BrokerId)
	opts.SetUsername(config.BrokerUsername)
	opts.SetPassword(config.BrokerPassword)

	client := mqtt.NewClient(opts)
	t.client = client
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	t.topicToHandler = make(map[string]*trigger.Handler)

	for _, handler := range t.handlers {

		topic := config.BrokerTopic

		if token := t.client.Subscribe(topic, 0, nil); token.Wait() && token.Error() != nil {
			log.Errorf("Error subscribing to topic %s: %s", topic, token.Error())
			return token.Error()
		} else {
			log.Debugf("Subscribed to topic: %s, will trigger handler: %s", topic, handler)
			t.topicToHandler[topic] = handler
		}
	}

	return nil
}

// Stop implements ext.Trigger.Stop
func (t *MqttTrigger) Stop() error {
	//unsubscribe from topic
	for _, handlerCfg := range t.config.Handlers {
		log.Debug("Unsubscribing from topic: ", handlerCfg.GetSetting("topic"))
		if token := t.client.Unsubscribe(handlerCfg.GetSetting("topic")); token.Wait() && token.Error() != nil {
			log.Errorf("Error unsubscribing from topic %s: %s", handlerCfg.Settings["topic"], token.Error())
		}
	}

	t.client.Disconnect(250)

	return nil
}


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
    BrokerPort string
    BrokerPassword string
    BrokerUsername string
	BrokerId string
	BrokerTopic string
}