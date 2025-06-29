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
    Client client;
    Index index;
    Logger logger = Logger.getLogger(this.getClass().getName());
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

            Config config = new Config(host, apiKey, new JacksonJsonHandler());
            client = new Client(config);

            // Try to get or create the index
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
        ObjectMapper mapper = new ObjectMapper();
        logger.info("ENTERED PUT-------->");
        List<JsonNode> docsToIndex = new ArrayList<>();
        List<String> docsToDelete = new ArrayList<>();

        for (SinkRecord record : records) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> valueMap = (Map<String, Object>) record.value();
                JsonNode event = mapper.convertValue(valueMap, JsonNode.class);

                JsonNode opNode = event.get("op");
                if (opNode == null) {
                    logger.warning("Missing 'op' field in record: " + event);
                    continue;
                }

                String op = opNode.asText();
                logger.info("Operation type: " + op);

                switch (op) {
                    case "c":
                    case "u":
                    case "r": {
                        JsonNode afterNode = event.get("after");
                        if (afterNode == null || afterNode.isNull()) {
                            logger.warning("Missing 'after' field for op: " + op + ", event: " + event);
                            continue;
                        }
                        docsToIndex.add(afterNode);
                        break;
                    }
                    case "d": {
                        JsonNode beforeNode = event.get("before");
                        if (beforeNode == null || beforeNode.isNull() || beforeNode.get("id") == null) {
                            logger.warning("Missing 'before.id' field for delete event: " + event);
                            continue;
                        }
                        String id = beforeNode.get("id").asText();
                        docsToDelete.add(id);
                        break;
                    }
                    default:
                        logger.warning("Unsupported operation: " + op);
                }

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error parsing record", e);
            }
        }

        try {
            if (!docsToIndex.isEmpty()) {
                String jsonPayload = mapper.writeValueAsString(docsToIndex);
                logger.info("Indexing " + docsToIndex.size() + " documents");
                index.addDocuments(jsonPayload);
            }

            if (!docsToDelete.isEmpty()) {
                logger.info("Deleting " + docsToDelete.size() + " documents");
                String[] ids = docsToDelete.toArray(new String[0]);
                index.deleteDocuments(Arrays.asList(ids));
            }

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while syncing to Meilisearch", e);
        }

    }

    @Override
    public void stop() {
        logger.info("Stopping MeilisearchSinkTask...");
        client = null;
        index = null;
    }

}
