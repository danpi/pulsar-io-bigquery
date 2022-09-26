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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.core.SourceContext;
import org.junit.Test;

/**
 * The bigquery source config test.
 */
public class BigQuerySourceConfigTest {
    @SuppressWarnings("checkstyle:WhitespaceAfter")
    @Test
    public void testSourceConfigLoad() {
        Map<String, Object> mapConfig = new HashMap<>();
        mapConfig.put("projectId", "test-project");
        mapConfig.put("datasetName", "test-dataset");
        mapConfig.put("tableName", "test-table");
        mapConfig.put("snapshotTime", 1662709773L);
        mapConfig.put("queueSize", 1000);
        mapConfig.put("maxParallelism", 4);
        mapConfig.put("selectedFields", "col1");
        mapConfig.put("filters", "id>10");
        mapConfig.put("retainOrdering", true);
        mapConfig.put("checkpointIntervalSeconds", 30);
        mapConfig.put("credentialJsonString", "test credentialJsonString");

        BigQuerySourceConfig config = BigQuerySourceConfig.load(mapConfig, mock(SourceContext.class));

        assertEquals(config.getProjectId(), "test-project");
        assertEquals(config.getDatasetName(), "test-dataset");
        assertEquals(config.getTableName(), "test-table");
        assertEquals(config.getSnapshotTime(), 1662709773L);
        assertEquals(config.getQueueSize(), 1000);
        assertEquals(config.getMaxParallelism(), 4);
        assertEquals(config.getFilters(), "id>10");
        assertEquals(config.getSelectedFields(), "col1");
        assertTrue(config.getRetainOrdering());
        assertEquals(config.getCheckpointIntervalSeconds(), 30);
        assertEquals(config.getCredentialJsonString(), "test credentialJsonString");
    }

    @Test
    public void testDefaultValueLoad() {
        Map<String, Object> mapConfig = new HashMap<>();
        mapConfig.put("projectId", "test-project");
        mapConfig.put("datasetName", "test-dataset");
        mapConfig.put("tableName", "test-table");

        BigQuerySourceConfig config = BigQuerySourceConfig.load(mapConfig, mock(SourceContext.class));

        assertEquals(config.getProjectId(), "test-project");
        assertEquals(config.getDatasetName(), "test-dataset");
        assertEquals(config.getTableName(), "test-table");
        assertEquals(config.getSnapshotTime(), -1);
        assertEquals(config.getQueueSize(), 10000);
        assertEquals(config.getMaxParallelism(), 1);
        assertNull(config.getFilters());
        assertNull(config.getSelectedFields());
        assertFalse(config.getRetainOrdering());
        assertEquals(config.getCheckpointIntervalSeconds(), 60);
        assertNull(config.getCredentialJsonString());
    }

    @Test
    public void testValidateRetainOrdering() {
        Map<String, Object> mapConfig = new HashMap<>();
        mapConfig.put("projectId", "test-project");
        mapConfig.put("datasetName", "test-dataset");
        mapConfig.put("tableName", "test-table");
        mapConfig.put("retainOrdering", true);
        mapConfig.put("maxParallelism", 4);

        try {
            BigQuerySourceConfig config = BigQuerySourceConfig.load(mapConfig, mock(SourceContext.class));
            config.validate();
            fail();
        } catch (Exception e) {
            assertEquals("if retainOrdering=true, maxParallelism should be set to 1.", e.getMessage());
        }
    }
}
