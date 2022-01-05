/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
/**
 * Adapted from https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/cdc-parent#deploying-the-connector
 */
package io.debezium.server.pubsub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

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
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.datacatalog.DataCatalogSchemaUtils;
import io.debezium.server.datacatalog.DebeziumSourceRecordToDataflowCdcFormatTranslator;
import io.debezium.server.CustomConsumerBuilder;

/**
 * Implementation of the consumer that delivers the messages into Google Pub/Sub destination.
 *
 * @author Jiri Pechanec
 */
@Named("pubsub")
@Dependent
public class PubSubChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PubSubChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.pubsub.";
    private static final String PROP_PROJECT_ID = PROP_PREFIX + "project.id";

    public static interface PublisherBuilder {
        Publisher get(ProjectTopicName topicName);
    }

    private String projectId;

    private final Map<String, Publisher> publishers = new HashMap<>();
    private final DebeziumSourceRecordToDataflowCdcFormatTranslator translator = new DebeziumSourceRecordToDataflowCdcFormatTranslator();

    private DataCatalogSchemaUtils.DataCatalogSchemaManager schemaUpdater;
    private PublisherBuilder publisherBuilder;

    @ConfigProperty(name = PROP_PREFIX + "ordering.enabled", defaultValue = "true")
    boolean orderingEnabled;

    @ConfigProperty(name = PROP_PREFIX + "datacatalog.enabled", defaultValue = "true")
    boolean datacatalogEnabled;

    @ConfigProperty(name = PROP_PREFIX + "pubsub.topic.prefix", defaultValue = "pg-cdc-test")
    String pubsubTopicPrefix;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    @Inject
    @CustomConsumerBuilder
    Instance<PublisherBuilder> customPublisherBuilder;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        projectId = config.getOptionalValue(PROP_PROJECT_ID, String.class).orElse(ServiceOptions.getDefaultProjectId());

        if (customPublisherBuilder.isResolvable()) {
            publisherBuilder = customPublisherBuilder.get();
            LOGGER.info("Obtained custom configured PublisherBuilder '{}'", customPublisherBuilder);
            return;
        }

        if (datacatalogEnabled) {
            // default mode is multi-topic, bc it's better to run multiple topic-aware DataCatalog schema updater with
            // a single topic inside than vice versa.
            schemaUpdater = DataCatalogSchemaUtils.getSchemaManager(projectId, pubsubTopicPrefix, false);
            LOGGER.info("Initialized Data Catalog API usage, used prefix {} for PubSub topics", pubsubTopicPrefix);
        }

        publisherBuilder = (t) -> {
            try {
                return Publisher.newBuilder(t)
                        .setEnableMessageOrdering(orderingEnabled)
                        .build();
            } catch (IOException e) {
                throw new DebeziumException(e);
            }
        };

        LOGGER.info("Using default PublisherBuilder '{}'", publisherBuilder);
    }

    @PreDestroy
    void close() {
        publishers.values().forEach(publisher -> {
            try {
                publisher.shutdown();
            } catch (Exception e) {
                LOGGER.warn("Exception while closing publisher: {}", e.getMessage());
            }
        });
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        final List<ApiFuture<String>> deliveries = new ArrayList<>();
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            final String topicName = streamNameMapper.map(record.destination());
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

            if (record.value() instanceof String) {
                pubsubMessage.setData(ByteString.copyFromUtf8((String) record.value()));
            } else if (record.value() instanceof byte[]) {
                pubsubMessage.setData(ByteString.copyFrom((byte[]) record.value()));
            }

            if (datacatalogEnabled) {
                // only thing we apparently support for now
                LOGGER.info("Trying to cast ChangeEvent to RecordChangeEvent");
                LOGGER.info("ChangeEvent is not a RecordChangeEvent, not much we can do.");
                Row updateRecord = translator.translate((SourceRecord) record);
                if (updateRecord == null) {
                    continue;
                } else {
                    Entry result = schemaUpdater.updateSchemaForTable(record.destination(), updateRecord.getSchema());
                    if (result == null) {
                        throw new InterruptedException(
                                "A problem occurred when communicating with Cloud Data Catalog");
                    }
                }
            }

            deliveries.add(publisher.publish(pubsubMessage.build()));
            committer.markProcessed(record);
        }
        List<String> messageIds;
        try {
            messageIds = ApiFutures.allAsList(deliveries).get();
        } catch (ExecutionException e) {
            throw new DebeziumException(e);
        }
        LOGGER.trace("Sent messages with ids: {}", messageIds);
        committer.markBatchFinished();
    }

    @Override
    public boolean supportsTombstoneEvents() {
        return false;
    }
}
