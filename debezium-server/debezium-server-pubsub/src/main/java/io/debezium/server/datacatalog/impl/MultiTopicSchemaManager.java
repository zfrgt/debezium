package io.debezium.server.datacatalog.impl;

import com.google.cloud.datacatalog.v1.Entry;
import com.google.cloud.datacatalog.v1.UpdateEntryRequest;
import io.debezium.server.datacatalog.DataCatalogSchemaManager;
import io.debezium.server.datacatalog.SchemaUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MultiTopicSchemaManager extends DataCatalogSchemaManager {

    private final String pubsubTopicPrefix;
    private static final Logger LOG = LoggerFactory.getLogger(MultiTopicSchemaManager.class);

    public MultiTopicSchemaManager(String gcpProject, String location, String pubsubTopicPrefix) {
        super(gcpProject, location);
        setupDataCatalogClient();
        this.pubsubTopicPrefix = pubsubTopicPrefix;
        dcCache = new ConcurrentHashMap<>();
        initializeCache(this.dcCache, this.pubsubTopicPrefix);
    }

    @Override
    public String getPubSubTopicForTable(String tableName) {
        return String.format("%s_%s", pubsubTopicPrefix, tableName);
    }

    @Override
    public Entry getOrUpdateTableDcSchema(String tableName, Schema beamSchema) {
        if (client == null) {
            throw new IllegalArgumentException("Data Catalog client is not ready, please check the cfg/logs.");
        }
        String pubsubTopic = getPubSubTopicForTable(tableName);

        ImmutablePair<Entry, Schema> beforeLookupEntry = dcCache.get(tableName);
        if (beforeLookupEntry != null) {
            if (similarSchemas(beforeLookupEntry.right, beamSchema)) {
                LOG.trace("Returning entry from cache, it's similar to what we already have.");
                return beforeLookupEntry.left;
            }
        }

        // otherwise, do the lookup stuff and proceed
        Entry beforeChangeEntry = lookupPubSubEntry(client, pubsubTopic, this.gcpProject);
        if (beforeChangeEntry == null) {
            throw new IllegalArgumentException("Entry is null, most probably debezium-server is down");
        }
        LOG.debug("Converting Beam schema {} into a Data Catalog schema", beamSchema);
        com.google.cloud.datacatalog.v1.Schema newEntrySchema = SchemaUtils.fromBeamSchema(beamSchema);
        LOG.debug("Beam schema converted to Data Catalog schema {}", newEntrySchema);
        LOG.debug("Publishing schema for table {} corresponding to topic {}", tableName, pubsubTopic);

        UpdateEntryRequest updateEntryRequest = UpdateEntryRequest.newBuilder()
                .setEntry(beforeChangeEntry.toBuilder().setSchema(newEntrySchema).build())
                .build();

        Entry resultingEntry = client.updateEntry(updateEntryRequest);
        dcCache.putIfAbsent(tableName, new ImmutablePair<>(resultingEntry, beamSchema));
        return resultingEntry;
    }

    @Override
    public ImmutablePair<Entry, Schema> lookupSchemaEntryForTable(String tableName) {
        return dcCache.get(tableName);
    }
}