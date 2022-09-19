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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.ecosystem.io.bigquery.exception.BQConnectorDirectFailException;
import org.apache.pulsar.ecosystem.io.bigquery.utils.JsonUtils;
import org.apache.pulsar.io.core.SourceContext;

/**
 * The wrapper class for checkpoint.
 */
@Slf4j
public class CheckpointManager {
    private TableViewWrapper tableViewWrapper;

    private SourceContext sourceContext;

    @Getter
    @Setter
    private StreamCheckpoint curStreamCheckpoint;

    @Getter
    private SessionCheckpoint sessionCheckpoint;

    public CheckpointManager(SourceContext sourceContext, TableViewWrapper tableViewWrapper) throws Exception {
        this.sourceContext = sourceContext;
        this.tableViewWrapper = tableViewWrapper;
    }

    public void updateSessionCheckpoint(SessionCheckpoint sessionCheckpoint) {
        String key = SessionCheckpoint.getCheckpointSessionKeyFormat(this.sourceContext.getSourceName());
        try {
            byte[] value = JsonUtils.JSON_MAPPER.get().writeValueAsBytes(sessionCheckpoint);
            Long start = System.currentTimeMillis();
            this.tableViewWrapper.writeMessage(key, value);
            Long current = System.currentTimeMillis();
            log.info("update session checkpoint complete @ {}, cost:{} ms for session: {}, "
                    + "checkpoint: {}", current, current - start, key, value);
        } catch (Exception e) {
            log.error("Failed to update session checkpoint for key:{},value:{}", key, sessionCheckpoint);
        }
    }

    public SessionCheckpoint getSessionCheckpoint(String source) throws Exception {
        if (this.sessionCheckpoint != null) {
            return this.sessionCheckpoint;
        }
        byte[] sessionBytes =
                tableViewWrapper.getMessageByTableView(SessionCheckpoint.getCheckpointSessionKeyFormat(source));
        if (sessionBytes == null) {
            throw new BQConnectorDirectFailException("session can not find");
        }
        String jsonString = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(sessionBytes)).toString();
        ObjectMapper mapper = JsonUtils.JSON_MAPPER.get();
        try {
            this.sessionCheckpoint = mapper.readValue(jsonString, SessionCheckpoint.class);
        } catch (IOException e) {
            log.error("Parse the checkpoint for stream {} failed, jsonString: {} ",
                    this.curStreamCheckpoint.getStream(), jsonString, e);
            throw new IOException("Parse checkpoint failed");
        }
        return this.sessionCheckpoint;
    }

    public void updateStreamCheckpoint(StreamCheckpoint streamCheckpoint) {
        String key = StreamCheckpoint.getCheckpointStreamKeyFormat(streamCheckpoint.getStream());
        try {
            byte[] value = JsonUtils.JSON_MAPPER.get().writeValueAsBytes(streamCheckpoint);
            Long start = System.currentTimeMillis();
            this.tableViewWrapper.writeMessage(key, value);
            this.curStreamCheckpoint = streamCheckpoint;
            Long current = System.currentTimeMillis();
            log.info("update stream checkpoint complete @ {}, cost:{} ms for stream: {}, "
                    + "checkpoint: {}", current, current - start, key, value);
        } catch (Exception e) {
            log.error("Failed to update stream checkpoint for key:{},value:{}", key, streamCheckpoint);
        }
    }

    public StreamCheckpoint getAndSetStreamCheckpoint(String stream) throws Exception {
        StreamCheckpoint streamCheckpointByRemote = getStreamCheckpoint(stream);
        StreamCheckpoint streamCheckpoint = new StreamCheckpoint(stream);
        if (streamCheckpointByRemote == null) {
            this.curStreamCheckpoint = streamCheckpoint;
        } else {
            this.curStreamCheckpoint = streamCheckpointByRemote;
        }
        return this.curStreamCheckpoint;
    }

    public StreamCheckpoint getStreamCheckpoint(String stream) throws IOException {
        StreamCheckpoint streamCheckpoint = null;
        byte[] value =
                this.tableViewWrapper.getMessageByTableView(StreamCheckpoint.getCheckpointStreamKeyFormat(stream));
        if (value == null) {
            return null;
        }
        String jsonString = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(value)).toString();
        ObjectMapper mapper = JsonUtils.JSON_MAPPER.get();
        try {
            streamCheckpoint = mapper.readValue(jsonString, StreamCheckpoint.class);
        } catch (IOException e) {
            log.error("Parse the checkpoint for stream {} failed, jsonString: {} ",
                    this.curStreamCheckpoint.getStream(), jsonString, e);
            throw new IOException("Parse checkpoint failed");
        }
        return streamCheckpoint;
    }
}