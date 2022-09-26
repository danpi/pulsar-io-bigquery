/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.bigquery.source.reader;

import static java.lang.String.format;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.ecosystem.io.bigquery.BigQuerySourceConfig;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorDirectFailException;

/**
 * The helper class which provides to create bigquery session.
 */
public class ReadSessionCreator {
    private final BigQuerySourceConfig config;
    private final BigQuery bigQuery;
    private final BigQueryReadClient bigQueryReadClient;

    public ReadSessionCreator(BigQuerySourceConfig config) {
        this.config = config;
        this.bigQuery = config.createBigQuery();
        this.bigQueryReadClient = config.createBigQueryReadClient();
    }

    public ReadSessionResponse create(String selectFields,
                                      String filters) throws Exception {
        TableId tableId = TableId.of(config.getProjectId(), config.getDatasetName(),
                config.getTableName());

        TableInfo tableInfo = bigQuery.getTable(tableId);
        TableInfo actualTable = getActualTable(tableInfo);

        String srcTable = tablePathStr(actualTable.getTableId());

        ReadSession.TableReadOptions options = buildReadOptions(selectFields, filters);

        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(srcTable)
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(options);
        CreateReadSessionRequest.Builder requestBuilder =
                CreateReadSessionRequest.newBuilder()
                        .setParent(getParent(tableId.getProject()))
                        .setReadSession(sessionBuilder.build())
                        .setMaxStreamCount(config.getMaxParallelism());
        ReadSession session = bigQueryReadClient.createReadSession(requestBuilder.build());
        return new ReadSessionResponse(session, actualTable);
    }

    private String tablePathStr(TableId tableId) {
        return format(
                "projects/%s/datasets/%s/tables/%s",
                tableId.getProject(), tableId.getDataset(), tableId.getTable());
    }

    private String getParent(String project) {
        return format("projects/%s", project);
    }

    private TableInfo getActualTable(TableInfo tableInfo) throws Exception {
        TableDefinition tableDefinition = tableInfo.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        if (TableDefinition.Type.TABLE == tableType) {
            return tableInfo;
        }
        if (inputTableIsView(tableInfo)) {
            //materializeViewToTable
            throw new BQConnectorDirectFailException(
                    format("Table type '%s' of table '%s.%s', materializeViewToTable is not support",
                            tableType, tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable()));
        } else {
            throw new BQConnectorDirectFailException(
                    format("Table type '%s' of table '%s.%s' is not supported", tableType,
                            tableInfo.getTableId().getDataset(), tableInfo.getTableId().getTable()));
        }
    }

    private boolean inputTableIsView(TableInfo tableInfo) {
        TableDefinition tableDefinition = tableInfo.getDefinition();
        TableDefinition.Type tableType = tableDefinition.getType();
        return TableDefinition.Type.VIEW == tableType || TableDefinition.Type.MATERIALIZED_VIEW == tableType;
    }

    private ReadSession.TableReadOptions buildReadOptions(String selectFields,
                                                          String filters) {
        ReadSession.TableReadOptions.Builder tableReadOptionsBuilder = ReadSession.TableReadOptions.newBuilder();

        tableReadOptionsBuilder.addAllSelectedFields(buildSelectedFields(selectFields));
        buildFilter(filters).ifPresent(tableReadOptionsBuilder::setRowRestriction);
        return tableReadOptionsBuilder.build();
    }

    @VisibleForTesting
    protected ImmutableList<String> buildSelectedFields(String selectedFields) {
        if (StringUtils.isEmpty(selectedFields)) {
            return ImmutableList.of();
        }
        String[] selectFieldArray = selectedFields.split(",");
        return selectFieldArray.length == 0
                ? ImmutableList.of() : ImmutableList.copyOf(selectFieldArray);
    }

    @VisibleForTesting
    protected Optional<String> buildFilter(String filters) {
        if (StringUtils.isEmpty(filters)) {
            return Optional.empty();
        }
        String[] filterArray = filters.split(",");
        return Optional.of(
                Arrays.stream(filterArray).map(filter -> "(" + filter + ")").collect(Collectors.joining(" AND ")));
    }

}
