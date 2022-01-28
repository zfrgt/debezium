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
 * Adapted from https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/cdc-parent#deploying-the-connector
 */
package io.debezium.server.datacatalog;

import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class DbzToBeamTranslator {

    // service columns from DBZ that still make it to BQ
    private static class DataflowCdcRowFormat {
        public static final String OPERATION = "operation";
        public static final String PRIMARY_KEY = "primaryKey";
        public static final String FULL_RECORD = "fullRecord";
        public static final String TABLE_NAME = "tableName";
        public static final String TIMESTAMP_MS = "timestampMs";
    }

    // PG - DBZ operation types
    private static final class Operation {
        public static final String INSERT = "INSERT";
        public static final String UPDATE = "UPDATE";
        public static final String DELETE = "DELETE";
        public static final String READ = "READ";
    }

    public static Row translate(SourceRecord record) {
        LOG.trace("Source Record from Debezium: {}", record);
        if (record == null) {
            LOG.trace("Record is null, returning null");
            return null;
        }

        Struct recordValue = (Struct) record.value();
        if (recordValue == null) {
            LOG.trace("Record value is null, returning null");
            return null;
        }

        String operation = translateOperation(recordValue.getString("op"));
        if (operation == null) {
            LOG.trace("Record operation is null or unsupported, returning null");
            return null;
        }

        String qualifiedTableName = record.topic();

        Row beforeValueRow = subStructToRowOrNull(recordValue, "before");
        Row afterValueRow = subStructToRowOrNull(recordValue, "after");
        LOG.trace("DBZ entry, before section [{}]", beforeValueRow);
        LOG.trace("DBZ entry, after section [{}]", afterValueRow);

        Row primaryKey = null;
        if (record.key() != null) {
            primaryKey = handleValue(record.keySchema(), record.key());
            LOG.trace("Primary Key Schema: {} | Primary Key Value: {}", primaryKey.getSchema(), primaryKey);
        }

        Long timestampMs = recordValue.getInt64("ts_ms");
        org.apache.beam.sdk.schemas.Schema.Builder schemaBuilder;

        if (!knownSchemas.containsKey(qualifiedTableName)) {
            if (afterValueRow != null) {
                LOG.debug("After value row is not null, DBZ entry looks like a change one, using [after] schema.");
                schemaBuilder = org.apache.beam.sdk.schemas.Schema.builder()
                        .addStringField(DataflowCdcRowFormat.OPERATION)
                        .addStringField(DataflowCdcRowFormat.TABLE_NAME)
                        .addField(
                                org.apache.beam.sdk.schemas.Schema.Field.nullable(
                                        DataflowCdcRowFormat.FULL_RECORD, FieldType.row(afterValueRow.getSchema())))
                        .addInt64Field(DataflowCdcRowFormat.TIMESTAMP_MS);
            } else if (beforeValueRow != null) {
                // nothing was detected in 'after' part, meaning record schema should not have changed. let's take from 'before' part.
                LOG.debug("After value row is null, but before is not. DBZ entry looks like an initial snapshot, using [before] schema.");
                schemaBuilder = org.apache.beam.sdk.schemas.Schema.builder()
                        .addStringField(DataflowCdcRowFormat.OPERATION)
                        .addStringField(DataflowCdcRowFormat.TABLE_NAME)
                        .addField(
                                org.apache.beam.sdk.schemas.Schema.Field.nullable(
                                        DataflowCdcRowFormat.FULL_RECORD, FieldType.row(beforeValueRow.getSchema())))
                        .addInt64Field(DataflowCdcRowFormat.TIMESTAMP_MS);
            } else {
                LOG.warn("Both after and before value rows of DBZ entry are null, adding just the DBZ service columns to the schema.");
                // both are null, skip the field, put at least some basic fields to the schema
                schemaBuilder = org.apache.beam.sdk.schemas.Schema.builder()
                        .addStringField(DataflowCdcRowFormat.OPERATION)
                        .addStringField(DataflowCdcRowFormat.TABLE_NAME)
                        .addInt64Field(DataflowCdcRowFormat.TIMESTAMP_MS);
            }

            if (primaryKey != null) {
                schemaBuilder.addRowField(DataflowCdcRowFormat.PRIMARY_KEY, primaryKey.getSchema());
            }
            knownSchemas.putIfAbsent(qualifiedTableName, schemaBuilder.build());
        }

        org.apache.beam.sdk.schemas.Schema finalBeamSchema = knownSchemas.get(qualifiedTableName);

        Row.Builder beamRowBuilder =
                Row.withSchema(finalBeamSchema)
                        .addValue(operation)
                        .addValue(qualifiedTableName)
                        .addValue(beforeValueRow)
                        .addValue(afterValueRow)
                        .addValue(timestampMs);

        if (primaryKey != null) {
            beamRowBuilder.addValue(primaryKey);
        }

        Row result = beamRowBuilder.build();
        LOG.trace("Built Beam row: {}", result.toString(true));
        return result;
    }

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DbzToBeamTranslator.class);

    private static final ConcurrentMap<String, org.apache.beam.sdk.schemas.Schema> knownSchemas = new ConcurrentHashMap<>();

    private static Row subStructToRowOrNull(Struct recordValue, String subStructName) {
        Struct subStruct = recordValue.getStruct(subStructName);
        if (subStruct == null) {
            return null;
        } else {
            return handleValue(subStruct.schema(), subStruct);
        }
    }

    private static String translateOperation(String op) {
        switch (op) {
            case "c":
                return Operation.INSERT;
            case "u":
                return Operation.UPDATE;
            case "d":
                return Operation.DELETE;
            case "r":
                return Operation.READ;
            default:
                LOG.warn("Unsupported DBZ entry operation: [{}], returning null.", op);
                return null;
        }
    }

    @SuppressWarnings("deprecation")
    private static org.apache.beam.sdk.schemas.Schema
        kafkaSchemaToBeamRowSchema(org.apache.kafka.connect.data.Schema schema) {
        assert schema.type() == Schema.Type.STRUCT;
        LOG.trace("Transforming Kafka connect schema [{}] to Beam Row schema", schema);

        org.apache.beam.sdk.schemas.Schema.Builder beamSchemaBuilder = org.apache.beam.sdk.schemas.Schema.builder();
        for (Field f : schema.fields()) {
            Schema.Type t = f.schema().type();
            LOG.trace("Transforming Kafka connect schema field type {} to Beam type", t.toString());

            org.apache.beam.sdk.schemas.Schema.Field beamField;
            switch (t) {
                case INT8:
                case INT16:
                    LOG.trace("Field name: [{}], Int8/Int16 detected, mapping to Beam's Int16", f.name());
                    if (f.schema().isOptional()) {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.INT16);
                    } else {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.INT16);
                    }
                    break;
                case INT32:
                    LOG.trace("Field name: [{}], Int32 detected, mapping to Beam's Int32", f.name());
                    if (f.schema().isOptional()) {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.INT32);
                    } else {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.INT32);
                    }
                    break;
                case INT64:
                    LOG.trace("Field name: [{}], Int64 detected, mapping to Beam's Int64", f.name());
                    if (f.schema().isOptional()) {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.INT64);
                    } else {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.INT64);
                    }
                    break;
                case FLOAT32:
                    LOG.trace("Field name: [{}], Float32 detected, mapping to Beam's Float", f.name());
                    if (f.schema().isOptional()) {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.FLOAT);
                    } else {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.FLOAT);
                    }
                    break;
                case FLOAT64:
                    LOG.trace("Field name: [{}], Float64 detected, mapping to Beam's Double", f.name());
                    if (f.schema().isOptional()) {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.DOUBLE);
                    } else {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.DOUBLE);
                    }
                    break;
                case BOOLEAN:
                    LOG.trace("Field name: [{}], Boolean detected, mapping to Beam's Boolean", f.name());
                    if (f.schema().isOptional()) {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.BOOLEAN);
                    } else {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.BOOLEAN);
                    }
                    break;
                case STRING:
                    LOG.trace("Field name: [{}], String detected, mapping to Beam's String", f.name());
                    if (f.schema().isOptional()) {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.STRING);
                    } else {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.STRING);
                    }
                    break;
                case BYTES:
                    LOG.trace("Field name: [{}], Bytes detected", f.name());
                    if (Decimal.LOGICAL_NAME.equals(f.schema().name())) {
                        LOG.trace("Field is Decimal, mapping to Beam Decimal type.");
                        if (f.schema().isOptional()) {
                            beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.DECIMAL);
                        } else {
                            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.DECIMAL);
                        }
                    } else {
                        LOG.debug("Field is not decimal, rendering it as Beam Bytes.");
                        if (f.schema().isOptional()) {
                            beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(f.name(), FieldType.BYTES);
                        } else {
                            beamField = org.apache.beam.sdk.schemas.Schema.Field.of(f.name(), FieldType.BYTES);
                        }
                    }
                    break;
                case STRUCT:
                    LOG.trace("Field name: [{}], Struct detected, defining the subtypes.", f.name());
                    if (f.schema().isOptional()) {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.nullable(
                                f.name(), FieldType.row(kafkaSchemaToBeamRowSchema(f.schema())));
                    } else {
                        beamField = org.apache.beam.sdk.schemas.Schema.Field.of(
                                f.name(), FieldType.row(kafkaSchemaToBeamRowSchema(f.schema())));
                    }
                    break;
                case MAP:
                    throw new DataException("Map types are not supported.");
                case ARRAY:
                    throw new DataException("Array types are not supported.");
                default:
                    throw new DataException(String.format("Unsupported data type: %s", t));
            }
            if (f.schema().name() != null && !f.schema().name().isEmpty()) {
                LOG.trace("Schema name [{}] detected", f.schema().name());
                FieldType fieldType = beamField.getType();
                fieldType = fieldType.withMetadata("logicalType", f.schema().name());
                beamField = beamField.withType(fieldType).withDescription(f.schema().name());
            }
            beamSchemaBuilder.addField(beamField);
        }
        org.apache.beam.sdk.schemas.Schema result = beamSchemaBuilder.build();
        LOG.trace("Built resulting beam schema: [{}]", result.toString());

        return result;
    }

    private static Row kafkaSourceRecordToBeamRow(Struct value, Row.Builder rowBuilder) {
        org.apache.beam.sdk.schemas.Schema beamSchema = rowBuilder.getSchema();
        LOG.trace("Translating Kafka source record [{}] to Beam row", value.toString());

        for (org.apache.beam.sdk.schemas.Schema.Field f : beamSchema.getFields()) {
            switch (f.getType().getTypeName()) {
                case INT16:
                    rowBuilder.addValue(value.getInt16(f.getName()));
                    break;
                case INT32:
                    rowBuilder.addValue(value.getInt32(f.getName()));
                    break;
                case INT64:
                    rowBuilder.addValue(value.getInt64(f.getName()));
                    break;
                case FLOAT:
                    rowBuilder.addValue(value.getFloat32(f.getName()));
                    break;
                case DOUBLE:
                    rowBuilder.addValue(value.getFloat64(f.getName()));
                    break;
                case BOOLEAN:
                    rowBuilder.addValue(value.getBoolean(f.getName()));
                    break;
                case STRING:
                    rowBuilder.addValue(value.getString(f.getName()));
                    break;
                case DECIMAL:
                    rowBuilder.addValue(value.get(f.getName()));
                    break;
                case BYTES:
                    rowBuilder.addValue(value.getBytes(f.getName()));
                    break;
                case ROW:
                    rowBuilder.addValue(kafkaSourceRecordToBeamRow(
                            value.getStruct(f.getName()), Row.withSchema(f.getType().getRowSchema())));
                    break;
                case MAP:
                    throw new DataException("Map types are not supported.");
                case ARRAY:
                    throw new DataException("Array types are not supported.");
                default:
                    throw new DataException(String.format("Unsupported data type: %s", f.getType().getTypeName()));
            }
        }
        return rowBuilder.build();
    }

    private static Row handleValue(Schema schema, Object value) {
        org.apache.beam.sdk.schemas.Schema beamSchema = kafkaSchemaToBeamRowSchema(schema);
        Row.Builder rowBuilder = Row.withSchema(beamSchema);
        return kafkaSourceRecordToBeamRow((Struct) value, rowBuilder);
    }
}