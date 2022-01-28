package io.debezium.server.datacatalog;

import com.google.api.gax.rpc.ApiException;
import com.google.api.pathtemplate.PathTemplate;
import com.google.cloud.datacatalog.v1.*;
import com.google.common.base.Strings;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public abstract class DataCatalogSchemaManager {
    protected final String gcpProject;
    protected final String location;
    protected DataCatalogClient client;
    protected ConcurrentMap<String, ImmutablePair<Entry, Schema>> dcCache;

    private static final String DATA_CATALOG_PUBSUB_URI_TEMPLATE = "//pubsub.googleapis.com/projects/%s/topics/%s";
    private static final Logger LOG = LoggerFactory.getLogger(DataCatalogSchemaManager.class);

    public abstract String getPubSubTopicForTable(String tableName);
    public abstract Entry getOrUpdateTableDcSchema(String tableName, Schema beamSchema);
    public abstract ImmutablePair<Entry, Schema> lookupSchemaEntryForTable(String tableName);

    protected String getGcpProject() {
        return gcpProject;
    }

    protected DataCatalogSchemaManager(String gcpProject, String location) {
        this.gcpProject = gcpProject;
        this.location = location;
    }

    protected void setupDataCatalogClient() {
        if (client != null) {
            return;
        }

        try {
            client = DataCatalogClient.create();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create a DataCatalogClient", e);
        }
    }

    protected Entry lookupPubSubEntry(DataCatalogClient client, String pubsubTopic, String gcpProject) {
        String linkedResource = String.format(DATA_CATALOG_PUBSUB_URI_TEMPLATE, gcpProject, pubsubTopic);
        LOG.debug("Looking up LinkedResource {}", linkedResource);
        LookupEntryRequest request = LookupEntryRequest.newBuilder().setLinkedResource(linkedResource).build();

        try {
            return client.lookupEntry(request);
        } catch (ApiException e) {
            LOG.error("Can't lookup entry via request {}", request);
            LOG.error("ApiException thrown by Data Catalog API:", e);
            return null;
        }
    }

    protected String entryGroupNameForTopic(String pubsubTopic) {
        return pubsubTopic
                .replaceAll("\\.", "_")
                .replaceAll("-", "_");
    }

    protected String sanitizeEntryName(String tableName) {
        // Remove the instance name
        String unsanitizedEntry = tableName.split("\\.", 2)[1];
        return unsanitizedEntry.replace('-', '_').replace('.', '_');
    }

    protected String formatEntryGroupName(String project, String location, String entryGroup) {
        final PathTemplate ENTRY_GROUP_PATH_TEMPLATE = PathTemplate.createWithoutUrlEncoding("projects/{project}/locations/{location}/entryGroups/{entry_group}");
        return ENTRY_GROUP_PATH_TEMPLATE.instantiate("project", project, "location", location, "entry_group", entryGroup);
    }

    protected boolean similarSchemas(Schema right, Schema left) {
        List<String> rFieldNames = right.getFieldNames();
        List<String> lFieldNames = left.getFieldNames();
        Collections.sort(rFieldNames);
        Collections.sort(lFieldNames);
        return rFieldNames.equals(lFieldNames);
    }

    protected void initializeCache(ConcurrentMap<String, ImmutablePair<Entry, Schema>> dcCache, String pubsubTopic) {
        // fill in the values already present on the DataCatalog side
        List<Entry> entries = new ArrayList<>();
        String formattedParent = formatEntryGroupName(gcpProject, location, entryGroupNameForTopic(pubsubTopic));
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

        LOG.debug("Fetched entries: {}, caching them all", entries);
        entries.forEach(
                entry -> dcCache.put(entry.getDescription(), new ImmutablePair<>(
                                entry, SchemaUtils.toBeamSchema(entry.getSchema())
                        )
                ));
    }
}