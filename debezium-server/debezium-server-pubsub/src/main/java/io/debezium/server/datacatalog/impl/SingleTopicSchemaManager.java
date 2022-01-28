package io.debezium.server.datacatalog.impl;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.datacatalog.v1.CreateEntryGroupRequest;
import com.google.cloud.datacatalog.v1.CreateEntryRequest;
import com.google.cloud.datacatalog.v1.Entry;
import com.google.cloud.datacatalog.v1.EntryGroup;
import com.google.cloud.datacatalog.v1.EntryGroupName;
import com.google.cloud.datacatalog.v1.LocationName;
import io.debezium.server.datacatalog.DataCatalogSchemaManager;
import io.debezium.server.datacatalog.SchemaUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class SingleTopicSchemaManager extends DataCatalogSchemaManager {

    private final EntryGroup entryGroup;
    private final String pubsubTopic;
    private Boolean entryGroupCreated = false;

    private static final Logger LOG = LoggerFactory.getLogger(MultiTopicSchemaManager.class);

    public SingleTopicSchemaManager(String gcpProject, String location, String pubsubTopic) {
        super(gcpProject, location);
        setupDataCatalogClient();
        dcCache = new ConcurrentHashMap<>();

        this.pubsubTopic = pubsubTopic;
        this.entryGroup = createEntryGroup();
        initializeCache(dcCache, this.pubsubTopic);
    }

    private EntryGroup createEntryGroup() {
        if (this.entryGroupCreated) {
            return this.entryGroup;
        }

        EntryGroup entryGroup = EntryGroup.newBuilder()
                .setDisplayName(String.format("CDC_Debezium_on_Dataflow_%s", entryGroupNameForTopic(pubsubTopic)))
                .setDescription("EntryGroup representing a set of change streams from tables being replicated for CDC.")
                .build();

        CreateEntryGroupRequest entryGroupRequest = CreateEntryGroupRequest.newBuilder()
                .setParent(LocationName.of(getGcpProject(), location).toString())
                .setEntryGroupId(entryGroupNameForTopic(pubsubTopic))
                .setEntryGroup(entryGroup)
                .build();

        try {
            LOG.trace("Creating EntryGroup [{}]", entryGroupRequest);
            EntryGroup createdEntryGroup = client.createEntryGroup(entryGroupRequest);
            LOG.trace("Created EntryGroup [{}], caching it", createdEntryGroup.toString());
            this.entryGroupCreated = true;
            return createdEntryGroup;
        } catch (AlreadyExistsException e) {
            return entryGroup;
        }
    }

    @Override
    public String getPubSubTopicForTable(String tableName) {
        return pubsubTopic;
    }

    @Override
    public Entry getOrUpdateTableDcSchema(String tableName, Schema beamSchema) {
        LOG.debug("Looking up schema for table {}", tableName);
        LOG.debug("Beam Schema in the request: [{}]", beamSchema);
        ImmutablePair<Entry, Schema> possiblyCached = this.lookupSchemaEntryForTable(tableName);
        LOG.debug("Got following schema from cache [{}]", possiblyCached);
        if (possiblyCached != null) {
            LOG.debug("Schema entry [{}], schema object [{}]", possiblyCached.left, possiblyCached.right);
        }

        if (possiblyCached != null && similarSchemas(possiblyCached.right, beamSchema)) {
            LOG.debug("Returning cached Data Catalog entry and schema, fields are the same there.");
            return possiblyCached.left;
        } else if (possiblyCached != null) {
            LOG.warn("Beam Schema in the cache: [{}] doesn't match with the requested one. Removing previous entry.", possiblyCached.right);
            dcCache.remove(tableName);
            LOG.debug("Beam schema for table name [{}] has changed since the last time, modifying it and putting to cache/DataCatalog.", tableName);
        }

        LOG.debug("Converting Beam schema {} into a Data Catalog schema", beamSchema);
        com.google.cloud.datacatalog.v1.Schema newEntrySchema = SchemaUtils.fromBeamSchema(beamSchema);
        LOG.debug("Beam schema converted to Data Catalog schema {}", newEntrySchema);
        LOG.info("Entry group name: {}", EntryGroupName.of(getGcpProject(), location, entryGroupNameForTopic(pubsubTopic)).toString());

        CreateEntryRequest createEntryRequest = CreateEntryRequest.newBuilder()
                        .setParent(EntryGroupName.of(getGcpProject(), location, entryGroupNameForTopic(pubsubTopic)).toString())
                        .setEntryId(sanitizeEntryName(tableName))
                        .setEntry(Entry.newBuilder().setSchema(newEntrySchema)
                                .setDescription(tableName)
                                .setUserSpecifiedType("BEAM_ROW_DATA")
                                .setUserSpecifiedSystem("DATAFLOW_CDC_ON_DEBEZIUM_DATA")
                                .build())
                .build();
        LOG.debug("CreateEntryRequest: {}", createEntryRequest);

        try {
            Entry resultingEntry = client.createEntry(createEntryRequest);
            dcCache.putIfAbsent(tableName, new ImmutablePair<>(resultingEntry, beamSchema));
            return resultingEntry;
        } catch (AlreadyExistsException e) {
            LOG.warn("Schema for table name {} already exists, ignoring the exception.", tableName);
            return createEntryRequest.getEntry();
        }
    }

    @Override
    public ImmutablePair<Entry, Schema> lookupSchemaEntryForTable(String tableName) {
        return dcCache.get(tableName);
    }
}