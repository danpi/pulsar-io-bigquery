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
package org.apache.pulsar.ecosystem.io.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorDirectFailException;
import org.apache.pulsar.ecosystem.io.bigquery.utils.IOConfigUtils;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * The configuration class for {@link BigQuerySource}.
 */
@Slf4j
@Data
public class BigQuerySourceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    @FieldDoc(required = true, defaultValue = "", help = "projectId is BigQuery project id.")
    private String projectId;

    @FieldDoc(required = true, defaultValue = "", help = "datasetName is BigQuery dataset name.")
    private String datasetName;

    @FieldDoc(required = true, defaultValue = "", help = "tableName is BigQuery table name.")
    private String tableName;

    @FieldDoc(required = false, defaultValue = "-1", help = "The snapshot time of the table. If not set, interpreted "
            + "as now.")
    private long snapshotTime;

    @FieldDoc(required = false, defaultValue = "10000", help = "The buffer queue size of the source.")
    private int queueSize;

    @FieldDoc(required = false, defaultValue = "1", help = "The max parallelism for reading."
            + "The maximal number of partitions to split the data into."
            + "Actual number may be less if BigQuery deems the data small enough.")
    private int maxParallelism;

    @FieldDoc(required = false, defaultValue = "", help = "Names of the fields in the table that should be read.")
    private String selectedFields;

    @FieldDoc(required = false, defaultValue = "", help = "A list of clauses that can filter the result of the table.")
    private String filters;

    @FieldDoc(required = false, defaultValue = "false", help = "if retainOrdering=true,a session will get only one "
            + "stream.")
    private Boolean retainOrdering;

    @FieldDoc(required = false, defaultValue = "60", help = "The checkpoint interval(in units of seconds). By "
            + "default, it is set to 60s. ")
    private int checkpointIntervalSeconds;

    @FieldDoc(required = false, defaultValue = "", sensitive = true, help =
            "Authentication key, use the environment variable to get the key when key is empty."
                    + "It is recommended to set this value to null,"
                    + "and then add the GOOGLE_APPLICATION_CREDENTIALS environment "
                    + "variable to point to the path of the authentication key json file"
                    + "Key acquisition reference: \n"
                    + "https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#before-you-begin")
    private String credentialJsonString;

    public static BigQuerySourceConfig load(Map<String, Object> map, SourceContext sourceContext) {
        return IOConfigUtils.loadWithSecrets(map, BigQuerySourceConfig.class, sourceContext);
    }

    public void validate() throws IllegalArgumentException {
        if (retainOrdering && maxParallelism > 1) {
            log.error("if retainOrdering=true, maxParallelism should be set to 1 than {}", maxParallelism);
            throw new IllegalArgumentException("if retainOrdering=true, maxParallelism should be set to 1.");
        }
    }

    public BigQuery createBigQuery() {
        if (credentialJsonString != null && !credentialJsonString.isEmpty()) {
            return BigQueryOptions.newBuilder().setCredentials(getGoogleCredentials()).build().getService();
        } else {
            return BigQueryOptions.getDefaultInstance().getService();
        }
    }

    public BigQueryReadClient createBigQueryReadClient() {
        try {
            if (credentialJsonString != null && !credentialJsonString.isEmpty()) {
                GoogleCredentials googleCredentials = getGoogleCredentials();
                BigQueryReadSettings settings =
                        BigQueryReadSettings.newBuilder().setCredentialsProvider(() -> googleCredentials).build();
                return BigQueryReadClient.create(settings);
            } else {
                return BigQueryReadClient.create();
            }
        } catch (IOException e) {
            throw new BQConnectorDirectFailException(e);
        }
    }

    private GoogleCredentials getGoogleCredentials() {
        try {
            GoogleCredentials googleCredentials = GoogleCredentials.fromStream(
                    new ByteArrayInputStream(credentialJsonString.getBytes(StandardCharsets.UTF_8)));
            return googleCredentials;
        } catch (IOException e) {
            throw new BQConnectorDirectFailException(e);
        }
    }
}