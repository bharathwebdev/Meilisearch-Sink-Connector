# 🔌 Kafka Meilisearch Sink Connector

This is a custom **Apache Kafka Sink Connector** that streams data from Kafka topics into [Meilisearch](https://www.meilisearch.com/). It supports create, update, and delete operations using Debezium-style CDC records (with `op`, `before`, and `after` fields).

## 🚀 Features

- ✅ Supports CDC events with `op` field: `c`, `u`, `d`, `r`
- ✅ Automatically creates index in Meilisearch if it doesn't exist
- ✅ Deletes documents using their `id` on delete (`op = d`)
- ✅ Uses Meilisearch Java SDK with Jackson JSON handler
- ✅ Works with any Kafka Connect-compatible runtime

## 🧱 Requirements

- Apache Kafka + Kafka Connect
- Meilisearch 1.0+
- Java 8+
- maven

## 📦 Build

Use maven to build the connector:

```bash
mvn clean build
```



## 🚀 Deploy the Connector

1. Copy the JAR file to your Kafka Connect `plugins` directory.

2. Restart the Kafka Connect worker.

3. Deploy the connector using REST API:

```bash
curl -X POST http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d '{
  "name": "meilisearch-sink-connector",
  "config": {
    "connector.class": "org.adsonly.MeilisearchSinkConnector",
    "topics": "my-topic",
    "meilisearch.host": "http://localhost:7700",
    "meilisearch.apiKey": "your-meilisearch-api-key",
    "meilisearch.index": "my-index"
  }
}'
```

## 🧪 Supported Event Format (Debezium style)

The connector expects CDC-style JSON with the following structure:

```json
{
  "op": "c",
  "after": {
    "id": "123",
    "title": "Organic Apples",
    "price": 20
  }
}
```

- `op = c/u/r`: will index the `after` document.
- `op = d`: will remove the document using `before.id`.

Example for delete:

```json
{
  "op": "d",
  "before": {
    "id": "123"
  }
}
```

## 📜 Fields Required

- `id` must exist inside `after` (for insert/update) or `before` (for delete).
- Missing `id` will be skipped with a warning log.

## 🛡 Error Handling

- Invalid records are logged and skipped
- Index auto-creation is supported
- Retry handled by Kafka Connect framework

## 📚 Logging

The task uses Java `Logger`. You can configure logging level via the Connect worker’s logging setup (`log4j.properties` or `logging.properties`).

## 🧰 Configuration Properties

| Property              | Description                                  | Required     |
|-----------------------|----------------------------------------------|--------------|
| `meilisearch.host`    | URL to Meilisearch server                    | ✅ Yes       |
| `meilisearch.apiKey`  | Meilisearch API key (optional if public)     | ❌ Optional  |
| `meilisearch.index`   | Meilisearch index name                       | ✅ Yes       |


## 🤝 Contributing

Pull requests are welcome! Please include tests and make sure it builds via maven.

## 📄 License

This project is licensed under the MIT License.

## 🙌 Credits

- [Apache Kafka](https://kafka.apache.org/)
- [Meilisearch](https://www.meilisearch.com/)
- [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
