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

import com.google.cloud.datacatalog.v1.ColumnSchema;
import com.google.cloud.datacatalog.v1.Schema;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableBiMap;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class SchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

    private static final ImmutableBiMap<String, TypeName> FIELD_TYPE_NAMES =
            ImmutableBiMap.<String, org.apache.beam.sdk.schemas.Schema.TypeName>builder()
                    .put("BOOL", org.apache.beam.sdk.schemas.Schema.TypeName.BOOLEAN)
                    .put("BYTES", org.apache.beam.sdk.schemas.Schema.TypeName.BYTES)
                    .put("DOUBLE", org.apache.beam.sdk.schemas.Schema.TypeName.DOUBLE)
                    .put("NUMERIC", org.apache.beam.sdk.schemas.Schema.TypeName.DECIMAL)
                    .put("INT16", org.apache.beam.sdk.schemas.Schema.TypeName.INT16)
                    .put("INT32", org.apache.beam.sdk.schemas.Schema.TypeName.INT32)
                    .put("INT64", org.apache.beam.sdk.schemas.Schema.TypeName.INT64)
                    .put("STRING", org.apache.beam.sdk.schemas.Schema.TypeName.STRING)
                    .put("TIMESTAMP", org.apache.beam.sdk.schemas.Schema.TypeName.DATETIME)
                    .build();

    private static final ImmutableBiMap<String, FieldType> LOGICAL_FIELD_TYPES =
            ImmutableBiMap.<String, org.apache.beam.sdk.schemas.Schema.FieldType>builder()
                    .put("DATE", org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(SqlTypes.DATE))
                    .put("TIME", org.apache.beam.sdk.schemas.Schema.FieldType.logicalType(SqlTypes.TIME))
                    .put("MAP<STRING,STRING>", FieldType.map(FieldType.STRING, FieldType.STRING))
                    .build();

    public static org.apache.beam.sdk.schemas.Schema toBeamSchema(Schema catalogSchema) {
        return beamSchemaFromColumnList(catalogSchema.getColumnsList());
    }

    public static Schema fromBeamSchema(org.apache.beam.sdk.schemas.Schema beamSchema) {
        List<ColumnSchema> catalogColumns =
                beamSchema.getFields().stream()
                        .map(SchemaUtils::fromBeamField)
                        .collect(Collectors.toList());

        return Schema.newBuilder().addAllColumns(catalogColumns).build();
    }

    private static org.apache.beam.sdk.schemas.Schema beamSchemaFromColumnList(List<ColumnSchema> cols) {
        org.apache.beam.sdk.schemas.Schema.Builder schemaBuilder = org.apache.beam.sdk.schemas.Schema.builder();
        cols.forEach(c -> schemaBuilder.addField(toBeamField(c)));
        return schemaBuilder.build();
    }

    private static org.apache.beam.sdk.schemas.Schema.Field toBeamField(ColumnSchema cs) {
        String name = cs.getColumn();
        FieldType beamFieldType = getBeamFieldType(cs);

        Field field = Field.of(name, beamFieldType);

        if (Strings.isNullOrEmpty(cs.getMode()) || "NULLABLE".equals(cs.getMode())) {
            field = field.withNullable(true);
        } else if ("REQUIRED".equals(cs.getMode())) {
            field = field.withNullable(false);
        } else if ("REPEATED".equals(cs.getMode())) {
            field = Field.of(name, FieldType.array(beamFieldType));
        } else {
            throw new UnsupportedOperationException("Field mode '" + cs.getMode() + "' is not supported (field '" + name + "')");
        }

        return field;
    }

    private static FieldType getBeamFieldType(ColumnSchema column) {
        String dcFieldType = column.getType();

        if (LOGICAL_FIELD_TYPES.containsKey(dcFieldType)) {
            return LOGICAL_FIELD_TYPES.get(dcFieldType);
        } else if (FIELD_TYPE_NAMES.containsKey(dcFieldType)) {
            return FieldType.of(FIELD_TYPE_NAMES.get(dcFieldType));
        }

        if ("STRUCT".equals(dcFieldType)) {
            org.apache.beam.sdk.schemas.Schema structSchema = beamSchemaFromColumnList(column.getSubcolumnsList());
            return FieldType.row(structSchema);
        }

        throw new UnsupportedOperationException("Field type '" + dcFieldType + "' is not supported (field '" + column.getColumn() + "')");
    }

    private static ColumnSchema fromBeamField(org.apache.beam.sdk.schemas.Schema.Field beamField) {
        LOG.trace("Mapping beam field [{}] to DataCatalog column schema.", beamField.toString());
        ColumnSchema.Builder columnBuilder = ColumnSchema.newBuilder();

        // in order to reduce the number of API calls to Data Catalog, we're making everything optional
        columnBuilder.setMode("NULLABLE");

        if (beamField.getType().getTypeName() == TypeName.ROW && beamField.getType().getRowSchema() != null) {
            LOG.trace("Beam field type is Row, corresponding Data Catalog type is STRUCT.");
            String columnType = "STRUCT";
            Schema subSchema = fromBeamSchema(beamField.getType().getRowSchema());
            return columnBuilder
                    .setColumn(beamField.getName())
                    .setType(columnType)
                    .addAllSubcolumns(subSchema.getColumnsList())
                    .build();
        } else if (LOGICAL_FIELD_TYPES.inverse().containsKey(beamField.getType())) {
            String columnType = LOGICAL_FIELD_TYPES.inverse().get(beamField.getType());
            LOG.trace("Field type is one of logical ones [{}].", columnType);
            return columnBuilder.setColumn(beamField.getName()).setType(columnType).build();
        } else {
            String columnType = FIELD_TYPE_NAMES.inverse().get(beamField.getType().getTypeName());
            LOG.warn("Row schema may be null or an unsupported one: [{}]", columnType);
            return columnBuilder.setColumn(beamField.getName()).setType(columnType).build();
        }
    }
}
