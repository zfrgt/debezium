/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/*
Adapted from https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/cdc-parent/cdc-common
 */
package io.debezium.server.datacatalog;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.pathtemplate.PathTemplate;
import com.google.cloud.datacatalog.v1.EntryGroupName;
import com.google.cloud.datacatalog.v1.CreateEntryGroupRequest;
import com.google.cloud.datacatalog.v1.CreateEntryRequest;
import com.google.cloud.datacatalog.v1.DataCatalogClient;
import com.google.cloud.datacatalog.v1.Entry;
import com.google.cloud.datacatalog.v1.EntryGroup;
import com.google.cloud.datacatalog.v1.ListEntriesRequest;
import com.google.cloud.datacatalog.v1.ListEntriesResponse;
import com.google.cloud.datacatalog.v1.LocationName;
import com.google.cloud.datacatalog.v1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1.UpdateEntryRequest;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class with utilities to communicate with Google Cloud Data Catalog. */
public class DataCatalogSchemaUtils {

    // TODO(pabloem)(#138): Avoid using a default location for schema catalog entities.

    /** Template for the URI of a Pub/Sub topic {@link Entry} in Data Catalog. */
    private static final String DATA_CATALOG_PUBSUB_URI_TEMPLATE =
            "//pubsub.googleapis.com/projects/%s/topics/%s";

    private static final Logger LOG = LoggerFactory.getLogger(DataCatalogSchemaUtils.class);

    /**
     * Builds an {@link EntryGroup} name for a particular pubsubTopic.
     *
     * <p>This method is intended for use in single-topic mode, where an {@link EntryGroup} with
     * multiple {@link Entry}s is created for a single Pub/Sub topic.
     */
    public static String entryGroupNameForTopic(String pubsubTopic) {
        return String.format("cdc_%s", pubsubTopic);
    }

    /** Build a {@link DataCatalogSchemaManager} given certain parameters. */
    public static DataCatalogSchemaManager getSchemaMgr(
            String gcpProject, String pubsubTopicPrefix, String region, Boolean singleTopic) {
        return getSchemaManager(gcpProject, pubsubTopicPrefix, region, singleTopic);
    }

    /** Build a {@link DataCatalogSchemaManager} given certain parameters. */
    public static DataCatalogSchemaManager getSchemaManager(
            String gcpProject, String pubsubTopicPrefix, String location, Boolean singleTopic) {
        if (singleTopic) {
            return new SingleTopicSchemaManager(gcpProject, location, pubsubTopicPrefix);
        } else {
            return new MultiTopicSchemaManager(gcpProject, location, pubsubTopicPrefix);
        }
    }

    /** Retrieve all of the {@link Schema}s associated to {@link Entry}s in an {@link EntryGroup}. */
    public static Map<String, Schema> getSchemasForEntryGroup(String gcpProject, String region, String entryGroupId) {
        DataCatalogClient client = null;
        try {
            client = DataCatalogClient.create();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create a DataCatalogClient", e);
        }
        if (client == null) {
            throw new IllegalArgumentException("Client is still null, check Data Catalog API logs and usage.");
        }

        String formattedParent = formatEntryGroupName(gcpProject, region, entryGroupId);

        List<Entry> entries = new ArrayList<>();
        ListEntriesRequest request = ListEntriesRequest.newBuilder().setParent(formattedParent).build();
        while (true) {
            ListEntriesResponse response = client.listEntriesCallable().call(request);
            entries.addAll(response.getEntriesList());
            String nextPageToken = response.getNextPageToken();
            if (!Strings.isNullOrEmpty(nextPageToken)) {
                request = request.toBuilder().setPageToken(nextPageToken).build();
            } else {
                break;
            }
        }

        LOG.debug("Fetched entries: {}", entries);

        return entries.stream()
                .collect(
                        Collectors.toMap(Entry::getDescription, e -> SchemaUtils.toBeamSchema(e.getSchema())));
    }

    /**
     * Retrieve the {@link Schema} associated to a Pub/Sub topics in an.
     *
     * <p>This method is to be used in multi-topic mode, where a single {@link Schema} is associated
     * to a single Pub/Sub topic.
     */
    public static Schema getSchemaFromPubSubTopic(String gcpProject, String pubsubTopic) {
        DataCatalogClient client = null;
        try {
            client = DataCatalogClient.create();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create a DataCatalogClient", e);
        }
        if (client == null) {
            return null;
        }

        Entry entry = lookupPubSubEntry(client, pubsubTopic, gcpProject);
        if (entry == null) {
            return null; // TODO(pabloem) Handle a failed entry lookup
        }

        return SchemaUtils.toBeamSchema(entry.getSchema());
    }

    static Entry lookupPubSubEntry(DataCatalogClient client, String pubsubTopic, String gcpProject) {
        String linkedResource =
                String.format(DATA_CATALOG_PUBSUB_URI_TEMPLATE, gcpProject, pubsubTopic);

        LOG.info("Looking up LinkedResource {}", linkedResource);

        LookupEntryRequest request =
                LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();

        try {
            return client.lookupEntry(request);
        } catch (ApiException e) {
            LOG.error("Can't lookup entry via request {}", request);
            LOG.error("ApiException thrown by Data Catalog API:", e);
            return null;
        }
    }

    /**
     * Abstract class to manage Data Catalog clients and schemas from the Debezium connector.
     *
     * <p>It provides methods to store and retrieve schemas for given source tables in Data Catalog.
     */
    public abstract static class DataCatalogSchemaManager {
        final String gcpProject;
        final String location;
        DataCatalogClient client;

        public abstract String getPubSubTopicForTable(String tableName);

        public abstract Entry updateSchemaForTable(String tableName, Schema beamSchema);

        public abstract ImmutablePair<Entry, Schema> lookupSchemaEntryForTable(String tableName);

        public String getGcpProject() {
            return gcpProject;
        }

        DataCatalogSchemaManager(String gcpProject, String location) {
            this.gcpProject = gcpProject;
            this.location = location;
        }

        void setupDataCatalogClient() {
            if (client != null) {
                return;
            }

            try {
                client = DataCatalogClient.create();
            } catch (IOException e) {
                throw new RuntimeException("Unable to create a DataCatalogClient", e);
            }
        }
    }

    static class SingleTopicSchemaManager extends DataCatalogSchemaManager {
        private final ConcurrentMap<String, ImmutablePair<Entry, Schema>> dcCache;

        private final String pubsubTopic;
        private Boolean entryGroupCreated = false;

        SingleTopicSchemaManager(String gcpProject, String location, String pubsubTopic) {
            super(gcpProject, location);
            this.pubsubTopic = pubsubTopic;
            this.dcCache = new ConcurrentHashMap<>();
            createEntryGroup();
        }

        private void createEntryGroup() {
            if (this.entryGroupCreated) {
                return;
            }
            setupDataCatalogClient();
            EntryGroup entryGroup =
                    EntryGroup.newBuilder()
                            .setDisplayName(String.format("CDC_Debezium_on_Dataflow_%s", pubsubTopic))
                            .setDescription(
                                    "This EntryGroup represents a set of change streams from tables "
                                            + "being replicated for CDC.")
                            .build();

            // Construct the EntryGroup request to be sent by the client.
            CreateEntryGroupRequest entryGroupRequest =
                    CreateEntryGroupRequest.newBuilder()
                            .setParent(LocationName.of(getGcpProject(), location).toString())
                            .setEntryGroupId(entryGroupNameForTopic(pubsubTopic))
                            .setEntryGroup(entryGroup)
                            .build();

            try {
                LOG.info("Creating EntryGroup {}", entryGroupRequest);
                EntryGroup createdEntryGroup = client.createEntryGroup(entryGroupRequest);
                LOG.info("Created EntryGroup: {}", createdEntryGroup.toString());

                this.entryGroupCreated = true;
            } catch (AlreadyExistsException e) {
                // EntryGroup already exists. There is no further action needed.
            }
        }

        @Override
        public String getPubSubTopicForTable(String tableName) {
            return pubsubTopic;
        }

        private static String sanitizeEntryName(String tableName) {
            // Remove the instance name
            String unsanitizedEntry = tableName.split("\\.", 2)[1];
            return unsanitizedEntry.replace('-', '_').replace('.', '_');
        }

        @Override
        public Entry updateSchemaForTable(String tableName, Schema beamSchema) {
            ImmutablePair<Entry, Schema> possiblyCached = this.lookupSchemaEntryForTable(tableName);
            if (possiblyCached != null && possiblyCached.right.equals(beamSchema)) {
                LOG.debug("Returning cached Data Catalog entry and schema.");
                return possiblyCached.left;
            } else {
                LOG.info("Beam schema for table name {} has changed since the last time, " +
                        "modifying it and putting to cache/DataCatalog.", tableName);
            }

            // otherwise, let's create a new one and put it into the cache
            LOG.debug("Converting Beam schema {} into a Data Catalog schema", beamSchema);
            com.google.cloud.datacatalog.v1.Schema newEntrySchema = SchemaUtils.fromBeamSchema(beamSchema);

            LOG.debug("Beam schema {} converted to Data Catalog schema {}", beamSchema, newEntrySchema);
            LOG.error(
                    "Entry group name: {}",
                    EntryGroupName.of(getGcpProject(), location, entryGroupNameForTopic(pubsubTopic))
                            .toString());

            CreateEntryRequest createEntryRequest =
                    CreateEntryRequest.newBuilder()
                            .setParent(
                                    EntryGroupName.of(getGcpProject(), location, entryGroupNameForTopic(pubsubTopic))
                                            .toString())
                            .setEntryId(sanitizeEntryName(tableName))
                            .setEntry(
                                    Entry.newBuilder()
                                            .setSchema(newEntrySchema)
                                            .setDescription(tableName)
                                            .setUserSpecifiedType("BEAM_ROW_DATA")
                                            .setUserSpecifiedSystem("DATAFLOW_CDC_ON_DEBEZIUM_DATA")
                                            .build())
                            .build();

            LOG.info("CreateEntryRequest: {}", createEntryRequest);

            try {
                Entry resultingEntry = client.createEntry(createEntryRequest);
                dcCache.put(tableName, new ImmutablePair<>(resultingEntry, beamSchema));
                return resultingEntry;
            } catch (AlreadyExistsException e) {
                // The Entry already exists. No further action is necessary.
                LOG.warn("Schema for table name {} already exists, ignoring the exception.", tableName, e);
                return createEntryRequest.getEntry();
            }
        }

        @Override
        public ImmutablePair<Entry, Schema> lookupSchemaEntryForTable(String tableName) {
            return dcCache.get(tableName);
        }
    }

    static class MultiTopicSchemaManager extends DataCatalogSchemaManager {

        private final String pubsubTopicPrefix;
        private final ConcurrentMap<String, ImmutablePair<Entry, Schema>> dcCache;

        MultiTopicSchemaManager(String gcpProject, String location, String pubsubTopicPrefix) {
            super(gcpProject, location);
            setupDataCatalogClient();
            this.pubsubTopicPrefix = pubsubTopicPrefix;
            this.dcCache = new ConcurrentHashMap<>();
        }

        @Override
        public String getPubSubTopicForTable(String tableName) {
            return String.format("%s_%s", pubsubTopicPrefix, tableName);
        }

        public Entry updateSchemaForTable(String tableName, Schema beamSchema) {
            if (client == null) {
                throw new IllegalArgumentException("Data Catalog client is not ready, please check the cfg/logs.");
            }
            String pubsubTopic = getPubSubTopicForTable(tableName);

            ImmutablePair<Entry, Schema> beforeLookupEntry = dcCache.get(tableName);
            if (beforeLookupEntry != null) {
                if (beforeLookupEntry.right == beamSchema) {
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
            LOG.debug("Beam schema {} converted to Data Catalog schema {}", beamSchema, newEntrySchema);

            LOG.info("Publishing schema for table {} corresponding to topic {}", tableName, pubsubTopic);
            UpdateEntryRequest updateEntryRequest =
                    UpdateEntryRequest.newBuilder()
                            .setEntry(beforeChangeEntry.toBuilder().setSchema(newEntrySchema).build())
                            .build();

            Entry resultingEntry = client.updateEntry(updateEntryRequest);
            dcCache.put(tableName, new ImmutablePair<>(resultingEntry, beamSchema));
            return resultingEntry;
        }

        @Override
        public ImmutablePair<Entry, Schema> lookupSchemaEntryForTable(String tableName) {
            return dcCache.get(tableName);
        }
    }

    private static String formatEntryGroupName(String project, String location, String entryGroup) {
        final PathTemplate ENTRY_GROUP_PATH_TEMPLATE = PathTemplate.createWithoutUrlEncoding("projects/{project}/locations/{location}/entryGroups/{entry_group}");
        return ENTRY_GROUP_PATH_TEMPLATE.instantiate("project", project, "location", location, "entry_group", entryGroup);
    }
}