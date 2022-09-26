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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.ecosystem.io.bigquery.source.checkpoint.CheckpointManager;
import org.apache.pulsar.ecosystem.io.bigquery.source.checkpoint.SessionCheckpoint;
import org.apache.pulsar.ecosystem.io.bigquery.source.checkpoint.StateType;
import org.apache.pulsar.ecosystem.io.bigquery.source.checkpoint.StreamCheckpoint;
import org.apache.pulsar.ecosystem.io.bigquery.source.checkpoint.TableViewWrapper;
import org.apache.pulsar.ecosystem.io.bigquery.source.reader.ReadSessionCreator;
import org.apache.pulsar.ecosystem.io.bigquery.source.reader.ReadSessionResponse;
import org.apache.pulsar.ecosystem.io.bigquery.source.reader.SourceReader;
import org.apache.pulsar.ecosystem.io.bigquery.source.record.ErrorNotifierRecord;
import org.apache.pulsar.ecosystem.io.bigquery.source.record.NullRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.SourceContext;

/**
 * BigQuery source impl.
 */
@Slf4j
public class BigQuerySource implements BatchSource<GenericRecord> {
    private static final int DEFAULT_QUEUE_LENGTH = 10000;
    private BigQuerySourceConfig bigQuerySourceConfig;
    private ExecutorService fetchRecordExecutor;
    private ScheduledExecutorService stateCheckExecutor;
    private ScheduledExecutorService checkpointExecutor;
    private SourceContext sourceContext;
    private LinkedBlockingQueue<Record<GenericRecord>> queue;
    private CheckpointManager checkpointManager;
    private SourceReader sourceReader;
    private AtomicInteger processingException = new AtomicInteger(0);

    private static final String FETCH_RECORD_THREAD = "pulsar-bigquery-source-fetch-record";
    private static final String CHECKPOINT_THREAD = "pulsar-bigquery-source-checkpoint";

    @Override
    public void open(Map<String, Object> config, SourceContext context) throws Exception {
        if (bigQuerySourceConfig != null) {
            log.error("The connector is already running, exiting!");
            throw new IllegalStateException("The connector is already running, exiting!");
        }
        this.sourceContext = context;

        this.bigQuerySourceConfig = BigQuerySourceConfig.load(config, sourceContext);
        this.bigQuerySourceConfig.validate();
        log.info("BigQuery source connector config:{}", bigQuerySourceConfig);

        this.queue = new LinkedBlockingQueue<>(this.getQueueLength());

        TableViewWrapper tableViewWrapper = new TableViewWrapper(sourceContext);
        this.checkpointManager = new CheckpointManager(sourceContext, tableViewWrapper);
        this.sourceReader = new SourceReader(bigQuerySourceConfig, queue, sourceContext, processingException);

        this.fetchRecordExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory(FETCH_RECORD_THREAD));
        this.checkpointExecutor =
                Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory(CHECKPOINT_THREAD));
        this.checkpointExecutor.scheduleAtFixedRate(this::updateCheckpoint,
                bigQuerySourceConfig.getCheckpointIntervalSeconds(),
                bigQuerySourceConfig.getCheckpointIntervalSeconds(), TimeUnit.SECONDS);

    }

    @Override
    public void discover(Consumer<byte[]> taskConsumer) throws Exception {
        log.info("Generating streamList with a session.");
        ReadSessionCreator readSessionCreator = new ReadSessionCreator(this.bigQuerySourceConfig);

        ReadSessionResponse session = readSessionCreator.create(this.bigQuerySourceConfig.getSelectedFields(),
                this.bigQuerySourceConfig.getFilters());
        log.info("session name={},discovered streams size={},streams={},row number={},avro schema={}",
                session.getReadSession().getName(), session.getReadSession().getStreamsList().size(),
                session.getReadSession().getStreamsList(), session.getReadTableInfo().getNumRows(),
                session.getReadSession().getAvroSchema().getSchema());

        SessionCheckpoint sessionCheckpoint = new SessionCheckpoint(session, this.sourceContext.getSourceName());
        if (session.getReadTableInfo().getNumRows().longValue() == 0
                || session.getReadSession().getStreamsList().size() == 0) {
            log.info("No data needs to be delivered from BigQuery");
            sessionCheckpoint.setStateType(StateType.FINISH);
            this.checkpointManager.updateSessionCheckpoint(sessionCheckpoint);
            return;
        }
        this.checkpointManager.updateSessionCheckpoint(sessionCheckpoint);
        this.stateCheckExecutor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("pulsar-bigquery-source-state-check"));
        this.stateCheckExecutor.scheduleAtFixedRate(this::stateCheck, 30, 30, TimeUnit.SECONDS);

        session.getReadSession().getStreamsList().forEach(readStream -> {
            taskConsumer.accept(readStream.getNameBytes().toByteArray());
            log.info("input task {} to taskConsumer", readStream.getName());
        });
    }

    @Override
    public void prepare(byte[] stream) throws Exception {
        String curStream = new String(stream, StandardCharsets.UTF_8);
        log.info("Instance " + this.sourceContext.getInstanceId() + " got a new discovered stream task={}", curStream);
        if (this.checkpointManager.getSessionCheckpoint() == null) {
            this.checkpointManager.getSessionCheckpoint(this.sourceContext.getSourceName());
        }
        log.info("sessionCheckpoint={}", this.checkpointManager.getSessionCheckpoint());
        StreamCheckpoint streamCheckpoint = this.checkpointManager.getAndSetStreamCheckpoint(curStream);
        log.info("curStreamName={},curIndex={},", streamCheckpoint.getStream(), streamCheckpoint.getOffset());

        sourceReader.setStream(streamCheckpoint, this.checkpointManager.getSessionCheckpoint().getBigQuerySchema());
        process();
    }

    private void process() {
        this.fetchRecordExecutor.execute(this.sourceReader);
    }

    @Override
    public Record<GenericRecord> readNext() throws Exception {
        if (this.processingException.get() > 0) {
            log.error("processing encounter exception will stop reading record and connector will exit");
            throw new Exception("processing exception in processing bigquery record");
        }
        Record<GenericRecord> record = this.queue.take();
        if (log.isDebugEnabled()) {
            log.debug("read next, record={}", record.getValue());
        }
        if (record instanceof ErrorNotifierRecord) {
            log.error("process error={}", ((ErrorNotifierRecord) record).getException().getMessage());
            throw ((ErrorNotifierRecord) record).getException();
        }
        if (record instanceof NullRecord) {
            log.info("read null,curStream finish,accept another stream.");
            StreamCheckpoint curStreamCheckpoint = this.checkpointManager.getCurrentStreamCheckpoint();
            curStreamCheckpoint.setOffset(this.sourceReader.getCurrentOffset());
            curStreamCheckpoint.setStateType(StateType.FINISH);
            this.sourceReader.setStateType(StateType.FINISH);
            this.checkpointManager.updateStreamCheckpoint(curStreamCheckpoint);
            return null;
        } else {
            return record;
        }
    }

    private void updateCheckpoint() {
        StreamCheckpoint curStreamCheckpoint = checkpointManager.getCurrentStreamCheckpoint();
        if (curStreamCheckpoint == null) {
            log.info("cur instance={} no stream", sourceContext.getInstanceId());
            return;
        } else if (curStreamCheckpoint.getStateType().equals(StateType.FINISH)) {
            log.info("stream had finish,stream={},endOffset={}", curStreamCheckpoint.getStream(),
                    curStreamCheckpoint.getOffset());
            return;
        }
        curStreamCheckpoint.setOffset(sourceReader.getMinUnAckOffset());
        curStreamCheckpoint.setStateType(sourceReader.getStateType());
        checkpointManager.updateStreamCheckpoint(curStreamCheckpoint);
    }

    private void stateCheck() {
        log.info("session stateCheck");
        SessionCheckpoint sessionCheckpoint = checkpointManager.getSessionCheckpoint();
        if (sessionCheckpoint == null || sessionCheckpoint.getStateType().equals(StateType.FINISH)) {
            log.info("state don't need to update,state={}", sessionCheckpoint);
            return;
        }
        AtomicInteger uncompletedStream = new AtomicInteger(sessionCheckpoint.getStreamList().size());
        AtomicLong completeCount = new AtomicLong(0);
        sessionCheckpoint.getStreamList().forEach(stream -> {
            try {
                StreamCheckpoint streamCheckpoint = checkpointManager.getStreamCheckpoint(stream);
                if (streamCheckpoint.getStateType().equals(StateType.FINISH)) {
                    uncompletedStream.decrementAndGet();
                }
                completeCount.addAndGet(streamCheckpoint.getOffset());
            } catch (IOException e) {
                log.error("stateCheck failed,e={}", e.getMessage());
            }
        });
        double finishRatio = (1.0 * completeCount.get()) / sessionCheckpoint.getTotalNum().longValue();
        long spentTime = System.currentTimeMillis() - sessionCheckpoint.getCreateTime();
        double estimatedTimeSeconds = (spentTime / finishRatio - spentTime) / 1000;
        log.info("finishRatio={}%,estimatedTimeSeconds={}", finishRatio * 100, estimatedTimeSeconds);
        if (uncompletedStream.get() == 0) {
            sessionCheckpoint.setStateType(StateType.FINISH);
            checkpointManager.updateSessionCheckpoint(sessionCheckpoint);
        }
    }

    @Override
    public void close() throws Exception {
        updateCheckpoint();
        log.info("batch source close");
    }

    public int getQueueLength() {
        return DEFAULT_QUEUE_LENGTH;
    }
}
