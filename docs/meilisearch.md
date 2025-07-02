# Kafka Meilisearch Sink Connector

The Kafka Meilisearch Sink Connector is a Kafka Connect connector that streams data from Kafka topics into [Meilisearch](https://www.meilisearch.com/), enabling real-time indexing and search over your Kafka data.

This connector is particularly useful for use cases involving product search, log analytics, or streaming catalog updates — where data is indexed and available for fast text-based querying.

---

## 🔧 How It Works

This connector consumes messages from Kafka topics and writes them into a Meilisearch index. It supports Debezium-style change events, handling `create`, `update`, and `delete` operations based on the `op` field in the record.

### Supported Operations

- `c`, `u`, `r` → Insert or update document using the `after` field
- `d` → Delete document using the `id` in the `before` field

---

## 🛠 Configuration

### Required Properties

| Property                  | Description                                        |
|---------------------------|----------------------------------------------------|
| `meilisearch.host`        | URL of the Meilisearch server                      |
| `meilisearch.index`       | Target index name in Meilisearch                   |
| `topics`                  | Kafka topics to consume from                       |
| `connector.class`         | Must be `org.adsonly.MeilisearchSinkConnector`     |

### Optional Properties

| Property                  | Description                         |
|---------------------------|-------------------------------------|
| `meilisearch.apiKey`      | API key for Meilisearch if required |

---

## 🧪 Example Message Format

This connector expects CDC-style messages. Below is an example of a record that creates or updates a document:

```json
{
  "op": "c",
  "after": {
    "id": "123",
    "title": "Bananas",
    "price": 10.5
  }
}
```

Example delete event:

```json
{
  "op": "d",
  "before": {
    "id": "123"
  }
}
```

---

## ⚙️ Sample Config (`meilisearch-connector.properties`)

```properties
name=meilisearch-sink-connector
connector.class=org.adsonly.MeilisearchSinkConnector
tasks.max=1
topics=products

meilisearch.host=http://localhost:7700
meilisearch.apiKey=your-api-key
meilisearch.index=products-index
```

---

## 🚀 Deployment Steps

1. Build the connector JAR with Gradle:

```bash
maven clean build
```

2. Copy the JAR to your Kafka Connect plugin path.

3. Deploy using the REST API or the config file.

---

## 🔐 Authentication

If your Meilisearch instance requires an API key, provide it using the `meilisearch.apiKey` property. Otherwise, leave it blank.

---

## 🛡 Error Handling

- Logs warnings for missing IDs or unknown `op` types
- Skips malformed records without crashing
- Automatically creates the index if it doesn't exist

---

## 💬 Support

Community support is available via [GitHub Issues](https://github.com/your-username/kafka-meilisearch-sink-connector/issues).
