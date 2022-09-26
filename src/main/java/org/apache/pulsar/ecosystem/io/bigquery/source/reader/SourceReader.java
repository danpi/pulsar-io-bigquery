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

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.ecosystem.io.bigquery.BigQuerySource;
import org.apache.pulsar.ecosystem.io.bigquery.BigQuerySourceConfig;
import org.apache.pulsar.ecosystem.io.bigquery.source.checkpoint.StateType;
import org.apache.pulsar.ecosystem.io.bigquery.source.checkpoint.StreamCheckpoint;
import org.apache.pulsar.ecosystem.io.bigquery.source.record.BigQueryRecord;
import org.apache.pulsar.ecosystem.io.bigquery.source.record.ErrorNotifierRecord;
import org.apache.pulsar.ecosystem.io.bigquery.source.record.NullRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;

/**
 * The source reader class for {@link BigQuerySource}.
 */
@Slf4j
public class SourceReader implements Runnable {
    private volatile boolean running;
    @Getter
    private String currentStream;
    @Getter
    private long currentOffset;
    @Getter
    @Setter
    private StateType stateType;
    private AtomicLong ackCount;
    private DatumReader<GenericRecord> datumReader;
    private BinaryDecoder decoder;
    private GenericRecord row;
    private String bigQuerySchema;
    private Schema bigQueryAvroSchema;
    private GenericSchema<org.apache.pulsar.client.api.schema.GenericRecord> pulsarSchema;
    private final LinkedBlockingQueue<Record<org.apache.pulsar.client.api.schema.GenericRecord>> queue;

    private final BigQueryReadClient bigQueryReadClient;

    private final SourceContext sourceContext;

    private StreamStateChecker streamStateChecker = null;
    private AtomicInteger processingException;

    public SourceReader(BigQuerySourceConfig config,
                        LinkedBlockingQueue<Record<org.apache.pulsar.client.api.schema.GenericRecord>> queue,
                        SourceContext sourceContext,
                        AtomicInteger processingException) {

        this.bigQueryReadClient = config.createBigQueryReadClient();
        this.queue = queue;
        this.sourceContext = sourceContext;
        this.processingException = processingException;
    }

    public void setStream(StreamCheckpoint checkpoint, String schema) {
        this.bigQuerySchema = schema;
        this.bigQueryAvroSchema = new Schema.Parser().parse(schema);
        this.pulsarSchema = convertToPulsarSchema(bigQueryAvroSchema, bigQuerySchema);
        this.datumReader = new GenericDatumReader<>(bigQueryAvroSchema);
        this.currentStream = checkpoint.getStream();
        this.currentOffset = checkpoint.getOffset();
        this.stateType = checkpoint.getStateType();
        this.ackCount = new AtomicLong(0);
        this.streamStateChecker = new StreamStateChecker(checkpoint.getStream());
        BigQueryRecord.setPulsarSchema(pulsarSchema);
    }

    @Override
    public void run() {
        ReadRowsRequest readRowsRequest =
                ReadRowsRequest.newBuilder().setReadStream(currentStream).setOffset(currentOffset).build();

        ServerStreamingCallable<ReadRowsRequest, ReadRowsResponse> callable = bigQueryReadClient.readRowsCallable();
        ServerStream<ReadRowsResponse> readRowsResponses = callable.call(readRowsRequest);
        for (ReadRowsResponse readRowsResponse : readRowsResponses) {
            if (readRowsResponse.hasAvroRows()) {
                try {
                    processRows(readRowsResponse.getAvroRows());
                } catch (IOException e) {
                    log.error("bigquery message read failed for ", e);
                    putQueue(new ErrorNotifierRecord(e));
                }
            }
        }
        streamStateChecker.notifySendAll();
        log.debug("all rows put to queue, waiting to complete, stream={} ,endOffset={}", currentStream,
                currentOffset);
        streamStateChecker.waitComplete();
        log.info("finished sending stream send to pulsar, stream={},ackCount={}", currentStream, ackCount);
        putQueue(null);
    }

    public void processRows(AvroRows avroRows) throws IOException {
        this.decoder = DecoderFactory.get().binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), decoder);
        while (!decoder.isEnd()) {
            GenericRecord rowRecord = datumReader.read(row, decoder);
            streamStateChecker.addSendingOffset(currentOffset);
            putQueue(new BigQueryRecord(rowRecord, sourceContext.getOutputTopic(), currentOffset, ackCount,
                    streamStateChecker, processingException));
            currentOffset++;
            log.info("offset=" + currentOffset + " body=" + rowRecord.toString());
        }
    }

    public void putQueue(Record<org.apache.pulsar.client.api.schema.GenericRecord> record) {
        try {
            if (record != null) {
                queue.put(record);
            } else {
                queue.put(new NullRecord());
            }
        } catch (InterruptedException e) {
            log.error("put record to queue failed,e={}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public long getMinUnAckOffset() {
        try {
            return streamStateChecker.getMinUnAckOffset();
        } catch (NoSuchElementException e) {
            log.info("stream not running");
            return 0;
        }
    }

    public static GenericSchema<org.apache.pulsar.client.api.schema.GenericRecord> convertToPulsarSchema(
            org.apache.avro.Schema bigQuerySchema,
            String bigQuerySchemaString) {
        SchemaInfo.SchemaInfoBuilder schemaInfoBuilder = SchemaInfo.builder();
        schemaInfoBuilder.name(bigQuerySchema.getName());
        schemaInfoBuilder.type(SchemaType.AVRO);
        schemaInfoBuilder.schema(bigQuerySchemaString.getBytes());

        return org.apache.pulsar.client.api.Schema.generic(schemaInfoBuilder.build());
    }

}
