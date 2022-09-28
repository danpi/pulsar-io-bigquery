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

import static org.mockito.Mockito.mock;
import com.google.cloud.bigquery.TableInfo;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.ecosystem.io.bigquery.BigQuerySourceConfig;
import org.apache.pulsar.io.core.SourceContext;

/**
 * ReadSessionCreator test. Just use the local debug test.
 */
public class ReadSessionCreatorTestDebug {
    private BigQuerySourceConfig config;
    private ReadSessionCreator readSessionCreator;

    private static final String querySql =
            "select product,inventory FROM `bigquery-dev-001.mydataset.Produce` WHERE inventory>=20";

    private static final String incorrectQuerySql =
            "select product,inventorys FROM `bigquery-dev-001.mydataset.Produce` WHERE inventory>=20";

    public static void main(String[] args) {
        ReadSessionCreatorTestDebug localDebug = new ReadSessionCreatorTestDebug();
        localDebug.setup();
        localDebug.normalFlowTest();
        localDebug.abnormalFlowTest();
    }

    public void normalFlowTest() {
        try {
            TableInfo tableInfo = this.readSessionCreator.materializeQueryToTable(querySql, 1);
            long expiration = tableInfo.getExpirationTime() - tableInfo.getCreationTime();
            System.out.println(expiration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void abnormalFlowTest() {
        try {
            TableInfo tableInfo = this.readSessionCreator.materializeQueryToTable(incorrectQuerySql, 1);
            long expiration = tableInfo.getExpirationTime() - tableInfo.getCreationTime();
            System.out.println(expiration);
        } catch (Exception e) {
            System.out.println("query sql failed");
            e.printStackTrace();
        }
    }

    public void setup() {
        Map<String, Object> mapConfig = new HashMap<>();
        mapConfig.put("projectId", "bigquery-dev-001");
        mapConfig.put("datasetName", "mydataset");
        mapConfig.put("tableName", "Produce");
        this.config = BigQuerySourceConfig.load(mapConfig, mock(SourceContext.class));
        this.readSessionCreator = new ReadSessionCreator(this.config);
    }
}
