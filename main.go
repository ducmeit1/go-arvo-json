package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"schema-registry-confluent/consumer"
	"schema-registry-confluent/producer"
)

type User struct {
	ID   int    `json:"id"`
	User string `json:"user"`
}

var (
	Topic             = "my-topic"
	SchemaRegistryUrl = "http://localhost:8081"
	Brokers           = "localhost:9092"
	AvroSchema        = `{
				"type": "record",
				"name": "user",
				"fields": [
					{"name": "id", "type": "int"},
					{"name": "user", "type": "string"}
				]
			}`
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func(ctx context.Context) {
		p, err := producer.NewProducer(Topic,
			Brokers,
			"test-sc-producer",
			SchemaRegistryUrl,
			producer.SetSchemaContent(AvroSchema),
			producer.SetSchemaType("AVRO"),
		)
		if err != nil {
			panic(err)
		}

		schema, err := p.FetchSchema(false)
		if err != nil {
			panic(err)
		}

		fmt.Println(fmt.Sprintf("Schema ID: %v", schema.ID()))

		schemaIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(schemaIDBytes, uint32(schema.ID()))

		id := 0

		for {
			select {
			case <-ctx.Done():
				return
			default:
				id++
				user := User{ID: id, User: fmt.Sprintf("User %d", id)}
				value, _ := json.Marshal(user)

				native, _, err := schema.Codec().NativeFromTextual(value)
				if err != nil {
					panic(err)
				}

				valueBytes, _ := schema.Codec().BinaryFromNative(nil, native)

				var recordValue []byte
				recordValue = append(recordValue, byte(0))
				recordValue = append(recordValue, schemaIDBytes...)
				recordValue = append(recordValue, valueBytes...)

				if err := p.ProduceMessage(recordValue); err != nil {
					fmt.Println(fmt.Sprintf("[Error] Produce message failed %v", err.Error()))
				}
			}
			time.Sleep(1 * time.Second)
		}
	}(ctx)

	go func(ctx context.Context) {
		time.Sleep(1 * time.Second)
		c, err := consumer.NewConsumer(Topic, Brokers, "test-sc-consumer", "earliest", SchemaRegistryUrl)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := c.Close(); err != nil {
				fmt.Println(fmt.Sprintf("[Error] Close consumer has error %v", err.Error()))
			}
		}()

		ec := c.Consume(true, func(message []byte) error {
			user := User{}
			err := json.Unmarshal(message, &user)
			if err != nil {
				fmt.Println(fmt.Sprintf("[Error] Parse after consume has fail %v", err.Error()))
			} else {
				fmt.Println(fmt.Sprintf("Got User ID: %v, User: %v", user.ID, user.User))
			}
			return nil
		})

		if ec != nil {
			panic(ec)
		}
	}(ctx)

	fmt.Println("Running")
	<-ctx.Done()
}
