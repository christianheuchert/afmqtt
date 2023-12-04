package mqtt

import (
	"github.com/project-flogo/core/data/coerce"
)


type Output struct {
	Message     string            `md:"message"`     // The message recieved
	Topic       string            `md:"topic"`       // The MQTT topic
	TopicParams map[string]string `md:"topicParams"` // The topic parameters
}

type Reply struct {
	Data interface{} `md:"data"` // The data to reply with
}

func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"message":     o.Message,
		"topic":       o.Topic,
		"topicParams": o.TopicParams,
	}
}

func (o *Output) FromMap(values map[string]interface{}) error {

	var err error
	o.Message, err = coerce.ToString(values["message"])
	if err != nil {
		return err
	}
	o.Topic, err = coerce.ToString(values["topic"])
	if err != nil {
		return err
	}
	o.TopicParams, err = coerce.ToParams(values["topicParams"])
	if err != nil {
		return err
	}

	return nil
}

func (r *Reply) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"data": r.Data,
	}
}

func (r *Reply) FromMap(values map[string]interface{}) error {

	r.Data = values["data"]
	return nil
}
