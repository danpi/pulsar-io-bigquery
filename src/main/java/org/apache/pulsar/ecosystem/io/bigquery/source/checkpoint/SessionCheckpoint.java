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

import com.google.cloud.bigquery.storage.v1.ReadStream;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.ecosystem.io.bigquery.source.reader.ReadSessionResponse;

/**
 * The session info checkpoint.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SessionCheckpoint {
    protected static final String CheckpointSessionKeyFormat = "checkpoint-session:%s";
    private String sourceName;
    private String session;
    private Long createTime;
    private StateType stateType;
    private List<String> streamList;
    private BigInteger totalNum;
    private long finishNum;
    private String schema;  //bigQuery schema

    public SessionCheckpoint(ReadSessionResponse session, String sourceName) {
        this.sourceName = sourceName;
        this.session = session.getReadSession().getName();
        this.streamList = session.getReadSession().getStreamsList().stream().map(ReadStream::getName).collect(
                Collectors.toList());
        this.totalNum = session.getReadTableInfo().getNumRows();
        this.schema = session.getReadSession().getAvroSchema().getSchema();
        this.createTime = System.currentTimeMillis();
        this.stateType = StateType.READING;
    }

    public static String getCheckpointSessionKeyFormat(String sourceName) {
        return String.format(CheckpointSessionKeyFormat, sourceName);
    }

    public void updateSessionCheckpointState(StateType stateType) {
        this.stateType = stateType;
    }
}