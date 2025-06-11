package org.adsonly;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MeilisearchSinkConnectorConfig extends AbstractConfig {

    public static final String MEILISEARCH_HOST_CONFIG = "meilisearch.host";
    public static final String MEILISEARCH_API_KEY_CONFIG = "meilisearch.api.key";
    public static final String MEILISEARCH_INDEX_CONFIG = "meilisearch.index";

    public static final ConfigDef CONFIG = new ConfigDef()
            .define(MEILISEARCH_HOST_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Meilisearch host URL")
            .define(MEILISEARCH_API_KEY_CONFIG, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Meilisearch API Key")
            .define(MEILISEARCH_INDEX_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Meilisearch Index Name");

    public MeilisearchSinkConnectorConfig(Map<String, ?> originals) {
        super(CONFIG, originals);
    }
}

