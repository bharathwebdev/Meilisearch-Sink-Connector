package org.adsonly;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meilisearch.sdk.Client;
import com.meilisearch.sdk.Config;
import com.meilisearch.sdk.Index;
import com.meilisearch.sdk.json.JacksonJsonHandler;
import com.meilisearch.sdk.model.TaskInfo;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MeilisearchSinkTask extends SinkTask {
    Client client;
    Index index;
    Logger logger = Logger.getLogger(this.getClass().getName());
    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> map) {
        Config config = new Config(map.get(MeilisearchSinkConnectorConfig.MEILISEARCH_HOST_CONFIG), map.get(MeilisearchSinkConnectorConfig.MEILISEARCH_API_KEY_CONFIG), new JacksonJsonHandler());
        client = new Client(config);
        String indexName = map.get(MeilisearchSinkConnectorConfig.MEILISEARCH_INDEX_CONFIG);
        try {
            index = client.getIndex(indexName);
        } catch (Exception e) {
            // If it doesn't exist, create it
            TaskInfo task = client.createIndex(indexName, "id");

            // Wait for the task to finish
            client.waitForTask(task.getTaskUid());

            // Now fetch the created index
            index = client.getIndex(indexName);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        ObjectMapper mapper = new ObjectMapper();
        logger.info("ENTERED PUT-------->");
        for (SinkRecord record : collection) {
            try {
                String value = record.value().toString();
                logger.info("EVENT DATA ------>");
                logger.log(Level.INFO,value);
                JsonNode event = mapper.convertValue(record.value(), JsonNode.class);
                String op = event.get("op").asText();

                if ("c".equals(op) || "u".equals(op)) {
                    String doc = mapper.writeValueAsString(event.get("after"));

                    doc = "["+doc+"]";

                    logger.info("DATA GETTING INSERTED-------->");
                    logger.info(doc);

                    index.addDocuments(doc);
                } else if ("d".equals(op)) {
                    String id = event.get("before").get("id").asText();
                    index.deleteDocument(id);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error processing record", e);
            }
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping MeilisearchSinkTask...");
        client = null;
        index = null;
    }

    private Map<String, Object> convertStructToMap(Struct struct) {
        Map<String, Object> map = new java.util.HashMap<>();
        for (Field field : struct.schema().fields()) {
            Object value = struct.get(field);
            if (value instanceof org.apache.kafka.connect.data.Struct) {
                map.put(field.name(), convertStructToMap((org.apache.kafka.connect.data.Struct) value));
            } else {
                map.put(field.name(), value);
            }
        }
        return map;
    }
}
