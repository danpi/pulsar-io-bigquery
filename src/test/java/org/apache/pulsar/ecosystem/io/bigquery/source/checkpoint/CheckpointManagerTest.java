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
package org.apache.pulsar.ecosystem.io.bigquery.source.checkpoint;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.AvroSchema;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.ecosystem.io.bigquery.source.reader.ReadSessionResponse;
import org.apache.pulsar.io.core.SourceContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

/**
 * The checkpoint manager test.
 */
@RunWith(MockitoJUnitRunner.class)
@PowerMockIgnore({"javax.management.*"})
public class CheckpointManagerTest {
    @InjectMocks
    private CheckpointManager checkpointManager;
    @Mock
    private TableViewWrapper tableViewWrapper;
    @Mock
    private SourceContext sourceContext;
    private Map<String, byte[]> checkpointCache = new HashMap<>();
    private static final String sourceName = "batchSourceDemo";

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        when(sourceContext.getSourceName()).thenReturn(sourceName);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] arguments = invocationOnMock.getArguments();
                String key = (String) arguments[0];
                return checkpointCache.get(key);
            }
        }).when(tableViewWrapper).getMessageByTableView(any());
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {

                Object[] arguments = invocationOnMock.getArguments();
                String key = (String) arguments[0];
                byte[] buffer = (byte[]) arguments[1];
                checkpointCache.put(key, buffer);
                return null;
            }
        }).when(tableViewWrapper).writeMessage(any(), any());
    }

    @Test
    public void testCheckpointSession() throws Exception {
        ReadSession readSession = mock(ReadSession.class);
        when(readSession.getName()).thenReturn("test-session");
        when(readSession.getStreamsList()).thenReturn(
                ImmutableList.of(ReadStream.newBuilder().setName("stream1").build()));
        AvroSchema avroSchema = buildSchema();
        when(readSession.getAvroSchema()).thenReturn(avroSchema);

        TableInfo tableInfo = mock(TableInfo.class);
        when(tableInfo.getNumRows()).thenReturn(BigInteger.valueOf(10));

        ReadSessionResponse readSessionResponse = new ReadSessionResponse(readSession, tableInfo);
        SessionCheckpoint sessionCheckpointInput = new SessionCheckpoint(readSessionResponse, sourceName);
        checkpointManager.updateSessionCheckpoint(sessionCheckpointInput);

        SessionCheckpoint sessionCheckpointOutput = checkpointManager.getSessionCheckpoint(sourceName);

        assertEquals(sessionCheckpointOutput.getSession(), sessionCheckpointInput.getSession());
        assertEquals(sessionCheckpointOutput.getSourceName(), sessionCheckpointInput.getSourceName());
        assertEquals(sessionCheckpointOutput.getTotalNum(), sessionCheckpointInput.getTotalNum());
        assertEquals(sessionCheckpointOutput.getBigQuerySchema(), sessionCheckpointInput.getBigQuerySchema());
    }

    private AvroSchema buildSchema() {
        return AvroSchema.newBuilder().setSchema("{\"type\": \"array\", \"items\": \"string\"}").build();
    }
}
