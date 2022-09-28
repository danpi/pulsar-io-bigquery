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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Table;
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
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.ecosystem.io.bigquery.BigQuerySourceConfig;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorDirectFailException;

/**
 * The helper class which provides to create bigquery session.
 */
@Slf4j
public class ReadSessionCreator {
    private static final String TEMP_TABLE_FORMAT = "_bqt_%s";
    private final BigQuerySourceConfig config;
    private final BigQuery bigQuery;
    private final BigQueryReadClient bigQueryReadClient;
    private final boolean isTemporaryTable;

    private TableId actualTableId;

    public ReadSessionCreator(BigQuerySourceConfig config) {
        this.config = config;
        this.bigQuery = config.createBigQuery();
        this.bigQueryReadClient = config.createBigQueryReadClient();
        this.isTemporaryTable = StringUtils.isNotEmpty(this.config.getSql());
    }

    public ReadSessionResponse createReadSession() throws BQConnectorDirectFailException {
        this.actualTableId =
                TableId.of(this.config.getProjectId(), this.config.getDatasetName(), this.config.getTableName());
        if (this.isTemporaryTable) {
            TableInfo sqlTableInfo =
                    materializeQueryToTable(this.config.getSql(), this.config.getExpirationTimeInMinutes());
            this.actualTableId = sqlTableInfo.getTableId();
        }
        return this.create(this.actualTableId);
    }

    private ReadSessionResponse create(TableId tableId) throws BQConnectorDirectFailException {
        TableInfo tableInfo = this.bigQuery.getTable(tableId);
        TableInfo actualTable = getActualTable(tableInfo);

        String srcTable = tablePathStr(actualTable.getTableId());

        ReadSession.TableReadOptions options =
                buildReadOptions(this.config.getSelectedFields(), this.config.getFilters());

        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(srcTable)
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(options);
        CreateReadSessionRequest.Builder requestBuilder =
                CreateReadSessionRequest.newBuilder()
                        .setParent(getParent(tableId.getProject()))
                        .setReadSession(sessionBuilder.build())
                        .setMaxStreamCount(this.config.getMaxParallelism());
        ReadSession session = this.bigQueryReadClient.createReadSession(requestBuilder.build());
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

    private TableInfo getActualTable(TableInfo tableInfo) throws BQConnectorDirectFailException {
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

    /**
     * materialize a table by the querySql.
     * @param querySql the sql query on BigQuery.
     * @param expirationTimeInMinutes the expiration time until the table is expired or auto-deleted.
     * @return bigQuery table information.
     */
    @VisibleForTesting
    protected TableInfo materializeQueryToTable(String querySql, int expirationTimeInMinutes)
            throws BQConnectorDirectFailException {
        TableId tableId = buildTemporaryTableId();
        JobInfo jobInfo = JobInfo.of(QueryJobConfiguration.newBuilder(querySql)
                .setDestinationTable(tableId)
                .build());
        log.debug("running query job info={}", jobInfo);
        Job job = waitForJob(this.bigQuery.create(jobInfo));
        if (job.getStatus().getError() != null) {
            log.error("materialize query to table failed,error={}", job.getStatus().getError());
            throw new BQConnectorDirectFailException(job.getStatus().getError().getMessage());
        }
        log.info("job has finished={}", job);
        //add expiration to the temporary table
        TableInfo table = this.bigQuery.getTable(tableId);
        long expirationTime = table.getCreationTime() + TimeUnit.MINUTES.toMillis(expirationTimeInMinutes);
        Table temporaryTable = this.bigQuery.update(table.toBuilder().setExpirationTime(expirationTime).build());
        return temporaryTable;
    }

    private TableId buildTemporaryTableId() {
        String temporaryTableName =
                String.format(TEMP_TABLE_FORMAT,
                        UUID.randomUUID().toString().toLowerCase(Locale.ENGLISH).replace(" - ", ""));
        return TableId.of(config.getProjectId(), config.getDatasetName(), temporaryTableName);
    }

    private Job waitForJob(Job job) throws BQConnectorDirectFailException {
        try {
            return job.waitFor();
        } catch (BigQueryException e) {
            log.error("wait for job failed,e={}", e.getError());
            throw new BQConnectorDirectFailException("query sql failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BQConnectorDirectFailException(String.format("Job %s has been interrupted", job.getJobId()), e);
        }
    }

    public void deleteTemporaryTable() {
        if (this.isTemporaryTable && this.actualTableId.getTable()
                .startsWith(StringUtils.substring(TEMP_TABLE_FORMAT, 0, 5))) {
            log.info("delete temporary table,tableId={}", actualTableId);
            this.bigQuery.delete(actualTableId);
        }
    }
}
