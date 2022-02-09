/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
/*
 * Adapted from https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/cdc-parent#deploying-the-connector
 */
package io.debezium.server.pubsub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.server.datacatalog.DataCatalogSchemaManager;
import org.apache.beam.sdk.coders.RowCoder;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.datacatalog.v1.Entry;
import org.apache.beam.sdk.values.Row;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.datacatalog.DataCatalogSchemaUtils;
import io.debezium.server.datacatalog.DbzToBeamTranslator;
import io.debezium.server.CustomConsumerBuilder;

/**
 * Implementation of the consumer that delivers the messages into Google Pub/Sub destination.
 *
 * @author Jiri Pechanec
 */
@Named("pubsub")
@Dependent
public class PubSubChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.pubsub.";
    private static final String PROP_PREFIX_ORDERING_ENABLED = PROP_PREFIX + "ordering.enabled";
    private static final String PROP_PREFIX_NULL_KEY = PROP_PREFIX + "null.key";
    private static final String PROP_MODE = PROP_PREFIX + "mode";
    private static final String PROP_TOPIC = PROP_PREFIX + "topic";
    private static final String PROP_TOPIC_PREFIX = PROP_TOPIC + ".prefix";
    private static final String PROP_PROJECT_ID = PROP_PREFIX + "project.id";
    private static final String PROP_PROJECT_REGION = PROP_PREFIX + "project.region";

    public interface PublisherBuilder {
        Publisher get(ProjectTopicName topicName);
    }

    private final ConcurrentMap<String, Publisher> publishers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, RowCoder> rowCoderMap = new ConcurrentHashMap<>();

    private String projectId;
    private DataCatalogSchemaManager schemaUpdater;
    private PublisherBuilder publisherBuilder;

    @ConfigProperty(name = PROP_PREFIX_ORDERING_ENABLED, defaultValue = "true")
    boolean orderingEnabled;

    @ConfigProperty(name = PROP_TOPIC_PREFIX, defaultValue = "dbz_cdc")
    String pubsubTopicPrefix;

    @ConfigProperty(name = PROP_MODE)
    String pubsubMode;

    @ConfigProperty(name = PROP_TOPIC)
    String pubsubTopic;

    @ConfigProperty(name = PROP_PREFIX_NULL_KEY, defaultValue = "default")
    String nullKey;

    @ConfigProperty(name = PROP_PROJECT_REGION, defaultValue = "us-west1")
    String region;

    @Inject
    @CustomConsumerBuilder
    Instance<PublisherBuilder> customPublisherBuilder;

    @PostConstruct
    void connect() {
        LOG.info("Fetching PubSub Change Consumer configuration from configuration.");
        final Config config = ConfigProvider.getConfig();
        projectId = config.getOptionalValue(PROP_PROJECT_ID, String.class).orElse(ServiceOptions.getDefaultProjectId());

        if (customPublisherBuilder.isResolvable()) {
            publisherBuilder = customPublisherBuilder.get();
            LOG.info("Obtained custom configured PublisherBuilder '{}'", customPublisherBuilder);
            return;
        }

        if (pubsubTopicPrefix != null && pubsubMode.equalsIgnoreCase("multi")) {
            schemaUpdater = DataCatalogSchemaUtils.getMultiTopicSchemaManager(projectId, region, pubsubTopicPrefix);
            LOG.info("Initialized Data Catalog API, used prefix {} for PubSub topics", pubsubTopicPrefix);
        } else {
            schemaUpdater = DataCatalogSchemaUtils.getSingleTopicSchemaManager(projectId, region, pubsubTopic);
            LOG.info("Initialized Data Catalog API, used PubSub topic: [{}]", pubsubTopic);
        }

        publisherBuilder = (t) -> {
            try {
                return Publisher.newBuilder(t).setEnableMessageOrdering(orderingEnabled).build();
            } catch (IOException e) {
                throw new DebeziumException(e);
            }
        };
        LOG.info("Connection successful. Using default PublisherBuilder '{}'", publisherBuilder);
    }

    @PreDestroy
    void close() {
        publishers.values().forEach(p -> {
            try {
                p.shutdown();
            } catch (Exception e) {
                LOG.warn("Exception while closing publisher: {}", e.getMessage());
            }
        });
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {

        final List<ApiFuture<String>> deliveries = new ArrayList<>();
        for (ChangeEvent<Object, Object> record : records) {
            LOG.trace("Received event '{}'", record);
            // supports multi-topic approach here
            final String topicName = streamNameMapper.map(schemaUpdater.getPubSubTopicForTable(record.destination()));
            Publisher publisher = publishers.computeIfAbsent(topicName, (x) -> publisherBuilder.get(ProjectTopicName.of(projectId, x)));
            final PubsubMessage.Builder pubsubMessage = PubsubMessage.newBuilder();

            if (orderingEnabled) {
                if (record.key() == null) {
                    pubsubMessage.setOrderingKey(nullKey);
                } else if (record.key() instanceof String) {
                    pubsubMessage.setOrderingKey((String) record.key());
                } else if (record.key() instanceof byte[]) {
                    pubsubMessage.setOrderingKeyBytes(ByteString.copyFrom((byte[]) record.key()));
                }
            }

            EmbeddedEngineChangeEvent<Object, Object> embeddedChangeEvent = (EmbeddedEngineChangeEvent<Object, Object>) record;

            // Debezium publishes updates for each table in a separate Kafka topic, which is the fully
            // qualified name of the Database table (e.g. dbInstanceName.databaseName.table_name).
            String tableName = embeddedChangeEvent.sourceRecord().topic();
            Row updateRecord = DbzToBeamTranslator.translate(embeddedChangeEvent.sourceRecord());

            if (updateRecord == null) {
                continue;
            } else {
                Entry result = schemaUpdater.getOrUpdateTableDcSchema(record.destination(), updateRecord.getSchema());
                if (result == null) {
                    throw new InterruptedException("A problem occurred when communicating with Cloud Data Catalog.");
                }
            }

            try {
                RowCoder recordCoder = rowCoderMap.computeIfAbsent(tableName, s -> RowCoder.of(updateRecord.getSchema()));
                if (recordCoder != null) {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    recordCoder.encode(updateRecord, outputStream);
                    ByteString encodedUpdate = ByteString.copyFrom(outputStream.toByteArray());
                    PubsubMessage msg = pubsubMessage.setData(encodedUpdate).putAttributes("table", tableName).build();
                    deliveries.add(publisher.publish(msg));
                } else {
                    LOG.error("Unable to create RowCoder basing on row schema: {}", updateRecord.getSchema());
                    return;
                }
            } catch (IOException e) {
                LOG.error("Caught exception {} when trying to encode record {}. Stopping processing.", e, updateRecord);
                return;
            }
            committer.markProcessed(record);
        }

        List<String> messageIds;
        try {
            messageIds = ApiFutures.allAsList(deliveries).get();
        } catch (ExecutionException e) {
            throw new DebeziumException(e);
        }
        LOG.trace("Sent messages with ids: {}", messageIds);
        committer.markBatchFinished();
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return false;
    }
}
