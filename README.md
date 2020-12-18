# Go - Convert Arvo - JSON

- Uses with Arvo Schema
- Marshal/Unmarshal Message Produce/Consumer by JSON

- Run Kafka Stack with Confluent:
```code
docker-compse up -d
```
- Schema Registry: `http://localhost:8081`
- Broker: `localhost:9092`
- Control Center: `localhost:9021`

- Run code with GCO_ENABLED=1:
```code
CGO_ENABLED=1 go run main.go
```