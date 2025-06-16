echo "Register the Debezium PostgreSQL Connector (example)"


curl -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "adsonly-postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "plugin.name": "pgoutput",
      "database.hostname": "host.docker.internal",
      "database.port": "56573",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "adsonly",
      
      "database.server.name": "adsonly_server",
      "table.include.list": "public.units", 
      "topic.prefix": "adsonly",
      "slot.name": "adsonly_slot",
      "publication.name": "adsonly_pub",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": false,
      "value.converter.schemas.enable": false
    }
  }' | jq






curl --location 'http://localhost:8083/connectors' \
--header 'Content-Type: application/json' \
--data '{
  "name": "meilisearch-sink",
  "config": {
    "connector.class": "org.adsonly.MeilisearchSinkConnector",
    "topics": "adsonly.public.units",
    "meilisearch.host": "http://meilisearch:7700",
     "meilisearch.api.key":"masterKey",
    "meilisearch.index": "units"
  }
}' | jq