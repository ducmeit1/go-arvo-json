package producer

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"

	"schema-registry-confluent/util"
)

type OptionFunc func(p *Producer) error

type Producer struct {
	topic                string
	producer             *kafka.Producer
	schemaRegistryClient *srclient.SchemaRegistryClient
	schemaContent        string
	schemaType           srclient.SchemaType
}

func NewProducer(topic string, brokers string, clientID string, schemaRegistryUrl string, opts ...OptionFunc) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         clientID,
		"acks":              "all",
	})

	if err != nil {
		return nil, err
	}

	sc := srclient.CreateSchemaRegistryClient(schemaRegistryUrl)

	producer := &Producer{
		topic:                topic,
		producer:             p,
		schemaRegistryClient: sc,
	}

	for _, opt := range opts {
		if err := opt(producer); err != nil {
			return nil, err
		}
	}

	producer.DeliverMessageAsAsync()

	return producer, nil
}

func SetSchemaFile(file string) OptionFunc {
	return func(p *Producer) error {
		content, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}
		p.schemaContent = string(content)
		schemaType := util.DetectSchemaFileType(file)
		if schemaType == "" {
			return fmt.Errorf("schema type of file %v is not supported", file)
		}
		p.schemaType = schemaType
		return nil
	}
}

func SetSchemaContent(content string) OptionFunc {
	return func(p *Producer) error {
		p.schemaContent = content
		return nil
	}
}

func SetSchemaType(schemaType string) OptionFunc {
	return func(p *Producer) error {
		st := srclient.SchemaType(schemaType)
		if st == "" {
			return fmt.Errorf("schema type %v is not support", st)
		}
		p.schemaType = st
		return nil
	}
}

func (p *Producer) FetchSchema(isKey bool) (*srclient.Schema, error) {
	schema, err := p.schemaRegistryClient.GetLatestSchema(p.topic, isKey)
	if schema == nil && p.schemaContent != "" {

		schema, err = p.schemaRegistryClient.CreateSchema(p.topic, p.schemaContent, p.schemaType, isKey)
		if err != nil {
			return schema, nil
		}
	}
	return schema, err
}

func (p *Producer) ProduceMessage(message []byte) error {
	key, _ := uuid.NewUUID()
	fmt.Println(string(message))
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.topic,
			Partition: kafka.PartitionAny,
		},
		Value:     message,
		Key:       []byte(key.String()),
		Timestamp: time.Now().UTC(),
	}, nil)
	if err != nil {
		return err
	}
	p.producer.Flush(15 * 1000)
	return nil
}

func (p *Producer) DeliverMessageAsAsync() {
	go func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				message := ev
				if ev.TopicPartition.Error != nil {
					fmt.Println(fmt.Printf("[Error] Delivering the message '%s'", message.Key))
				} else {
					fmt.Println(fmt.Printf("Message '%s' delivered successfully!", message.Key))
				}
			}
		}
	}()
}
