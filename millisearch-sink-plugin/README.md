# Kafka Meilisearch Sink Connector

This is a custom Apache Kafka Sink Connector that streams data from Kafka topics directly into [Meilisearch](https://www.meilisearch.com/).

## 🔧 Features

- Supports Debezium-style CDC events (`c`, `u`, `r`, `d`)
- Automatically creates the index if it does not exist
- Batches add and delete operations
- Uses Meilisearch's `id` field for document identity

## 🚀 Configuration

Example `connector-config.json`:

```json
{
  "name": "meilisearch-sink-connector",
  "connector.class": "org.adsonly.MeilisearchSinkConnector",
  "tasks.max": "1",
  "topics": "my-topic",
  "meilisearch.host": "http://localhost:7700",
  "meilisearch.apiKey": "my-api-key",
  "meilisearch.index": "my-index"
}
```
