package org.adsonly;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.meilisearch.sdk.Client;
import com.meilisearch.sdk.Config;
import com.meilisearch.sdk.Index;
import com.meilisearch.sdk.json.JacksonJsonHandler;
import com.meilisearch.sdk.model.TaskInfo;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MeilisearchSinkTask extends SinkTask {

    private Client client;
    private Index index;
    private Logger logger = Logger.getLogger(this.getClass().getName());
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("Starting MeilisearchSinkTask...");

        try {
            String host = props.get(MeilisearchSinkConnectorConfig.MEILISEARCH_HOST_CONFIG);
            String apiKey = props.get(MeilisearchSinkConnectorConfig.MEILISEARCH_API_KEY_CONFIG);
            String indexName = props.get(MeilisearchSinkConnectorConfig.MEILISEARCH_INDEX_CONFIG);

            logger.info("Connecting to Meilisearch at: " + host + ", Index: " + indexName);

            Config config = new Config(host, apiKey, new JacksonJsonHandler());
            client = new Client(config);

            try {
                index = client.getIndex(indexName);
            } catch (Exception e) {
                TaskInfo task = client.createIndex(indexName, "id");
                client.waitForTask(task.getTaskUid());
                index = client.getIndex(indexName);
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to initialize Meilisearch client", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (client == null || index == null) {
            logger.warning("Client or Index is not initialized. Skipping records.");
            return;
        }

        List<JsonNode> docsToIndex = new ArrayList<>();
        List<String> docsToDelete = new ArrayList<>();

        for (SinkRecord record : records) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> valueMap = (Map<String, Object>) record.value();
                JsonNode event = mapper.convertValue(valueMap, JsonNode.class);

                String op = event.has("op") ? event.get("op").asText() : null;
                if (op == null) {
                    logger.warning("Missing 'op' field in record: " + event);
                    continue;
                }

                switch (op) {
                    case "c":
                    case "u":
                    case "r":
                        JsonNode after = event.get("after");
                        if (after == null || !after.has("id")) {
                            logger.warning("Missing 'id' in 'after' record: " + after);
                            continue;
                        }
                        docsToIndex.add(after);
                        break;
                    case "d":
                        JsonNode before = event.get("before");
                        if (before == null || !before.has("id")) {
                            logger.warning("Missing 'id' in 'before' record for delete: " + before);
                            continue;
                        }
                        docsToDelete.add(before.get("id").asText());
                        break;
                    default:
                        logger.warning("Unknown operation type: " + op);
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error processing record", e);
            }
        }

        try {
            if (!docsToIndex.isEmpty()) {
                String json = mapper.writeValueAsString(docsToIndex);
                TaskInfo task = index.addDocuments(json);
                client.waitForTask(task.getTaskUid());
                logger.info("Indexed " + docsToIndex.size() + " documents.");
            }

            if (!docsToDelete.isEmpty()) {
                TaskInfo task = index.deleteDocuments(docsToDelete);
                client.waitForTask(task.getTaskUid());
                logger.info("Deleted " + docsToDelete.size() + " documents.");
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error syncing documents to Meilisearch", e);
        }
    }

    @Override
    public void stop() {
        logger.info("Stopping MeilisearchSinkTask...");
        client = null;
        index = null;
    }
}
