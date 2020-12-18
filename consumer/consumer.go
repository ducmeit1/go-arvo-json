package consumer

import (
	"encoding/binary"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
)

type OptionFunc func(p *Consumer) error

type Consumer struct {
	topic                string
	consumer             *kafka.Consumer
	schemaRegistryClient *srclient.SchemaRegistryClient
}

func NewConsumer(topic string, brokers string, group string, offset string, schemaRegistryUrl string, opts ...OptionFunc) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          group,
		"auto.offset.reset": offset,
	})

	if err != nil {
		return nil, err
	}

	sc := srclient.CreateSchemaRegistryClient(schemaRegistryUrl)

	consumer := &Consumer{
		topic:                topic,
		consumer:             c,
		schemaRegistryClient: sc,
	}

	for _, opt := range opts {
		if err := opt(consumer); err != nil {
			return nil, err
		}
	}

	return consumer, nil
}

// TODO: Implement adding context and graceful shutdown
func (c *Consumer) Consume(useSchema bool, fn func(message []byte) error) error {
	err := c.consumer.SubscribeTopics([]string{c.topic}, nil)
	if err != nil {
		return err
	}

	if useSchema {
		for {
			msg, err := c.consumer.ReadMessage(-1)
			if err == nil {
				schemaID := binary.BigEndian.Uint32(msg.Value[1:5])
				schema, err := c.schemaRegistryClient.GetSchema(int(schemaID))
				if err != nil {
					panic(fmt.Sprintf("[Error] Getting the schema with id '%d' %s", schemaID, err.Error()))
				}
				native, _, _ := schema.Codec().NativeFromBinary(msg.Value[5:])
				value, _ := schema.Codec().TextualFromNative(nil, native)
				if ec := fn(value); ec != nil {
					fmt.Println(fmt.Sprintf("[ERROR] Message consumed has error: %v", ec.Error()))
				}
			} else {
				fmt.Println(fmt.Sprintf("[Error] Having problem when consuming message from brokers: %v", err.Error()))
			}
		}
	} else {
		for {
			msg, err := c.consumer.ReadMessage(-1)
			if err == nil {
				if ec := fn(msg.Value); ec != nil {
					fmt.Println(fmt.Sprintf("[ERROR] Message consumed has error: %v", ec.Error()))
				}
			} else {
				fmt.Println(fmt.Sprintf("[Error] Having problem when consuming message from brokers: %v", err.Error()))
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
