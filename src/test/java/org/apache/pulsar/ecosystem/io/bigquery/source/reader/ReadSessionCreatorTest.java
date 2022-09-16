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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.apache.pulsar.ecosystem.io.bigquery.BigQuerySourceConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * The read session creator test.
 */
public class ReadSessionCreatorTest {
    @InjectMocks
    private ReadSessionCreator readSessionCreator;
    @Mock
    private BigQuerySourceConfig bigQuerySourceConfig;
    @Mock
    private BigQuery bigQuery;
    @Mock
    private BigQueryReadClient bigQueryReadClient;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(bigQuerySourceConfig.createBigQuery()).thenReturn(bigQuery);
        when(bigQuerySourceConfig.createBigQueryReadClient()).thenReturn(bigQueryReadClient);
    }

    @Test
    public void testBuildSelectedFields() {
        String selectedFields = null;
        ImmutableList<String> selectedFieldList = readSessionCreator.buildSelectedFields(selectedFields);
        assertEquals(ImmutableList.of(), selectedFieldList);

        selectedFields = "id";
        selectedFieldList = readSessionCreator.buildSelectedFields(selectedFields);
        assertEquals(ImmutableList.of("id"), selectedFieldList);

        selectedFields = "id,name";
        selectedFieldList = readSessionCreator.buildSelectedFields(selectedFields);
        assertEquals(ImmutableList.of("id", "name"), selectedFieldList);
    }

    @Test
    public void testBuildFilter() {
        String filters = null;
        Optional<String> filter = readSessionCreator.buildFilter(filters);
        assertFalse(filter.isPresent());

        filters = "id>10";
        filter = readSessionCreator.buildFilter(filters);
        assertEquals("(id>10)", filter.get());

        filters = "id>10,age>18";
        filter = readSessionCreator.buildFilter(filters);
        assertEquals("(id>10) AND (age>18)", filter.get());
    }
}