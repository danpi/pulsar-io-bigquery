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
package org.apache.pulsar.ecosystem.io.bigquery.source.record;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.ecosystem.io.bigquery.source.reader.StreamStateChecker;
import org.apache.pulsar.functions.api.Record;

/**
 * The record wrapping an BigQuery message.
 */
@Slf4j
public class BigQueryRecord implements Record<GenericRecord> {
    private static GenericSchema<GenericRecord> pulsarSchema;
    private GenericRecord value;
    private String topic;
    private long offset;
    private AtomicLong ackCount;
    private org.apache.avro.generic.GenericRecord rowRecordData;
    private StreamStateChecker streamStateChecker;
    private AtomicInteger processingException;


    public BigQueryRecord(org.apache.avro.generic.GenericRecord rowRecordData,
                          String topic,
                          long offset,
                          AtomicLong ackCount,
                          StreamStateChecker streamStateChecker,
                          AtomicInteger processingException) {
        this.rowRecordData = rowRecordData;
        this.topic = topic;
        this.offset = offset;
        this.ackCount = ackCount;
        this.streamStateChecker = streamStateChecker;
        this.processingException = processingException;
        value = getGenericRecord(this.rowRecordData);
    }

    public static void setPulsarSchema(GenericSchema<GenericRecord> pulsarSchema) {
        BigQueryRecord.pulsarSchema = pulsarSchema;
    }

    @Override
    public Optional<String> getTopicName() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getKey() {
        return Optional.empty();
    }

    @Override
    public Schema<GenericRecord> getSchema() {
        return pulsarSchema;
    }

    @Override
    public GenericRecord getValue() {
        return value;
    }

    @Override
    public void ack() {
        if (log.isDebugEnabled()) {
            log.debug("send message success offset={}", offset);
        }
        ackCount.incrementAndGet();
        if (ackCount.get() % 10000 == 0) {
            log.info("send message success,offset={},ackCount={}", offset, ackCount.get());
        }
        streamStateChecker.updateAckOffset(offset);
    }

    @Override
    public void fail() {
        log.error("send message offset={} failed", offset);
        streamStateChecker.updateAckOffset(offset);
        processingException.incrementAndGet();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return Optional.of(this.topic);
    }

    @Override
    public Optional<Message<GenericRecord>> getMessage() {
        return Optional.empty();
    }

    public static GenericRecord getGenericRecord(org.apache.avro.generic.GenericRecord rowRecordData) {
        GenericRecordBuilder builder = pulsarSchema.newRecordBuilder();
        org.apache.avro.Schema avroSchema =
                new org.apache.avro.Schema.Parser().parse(new String(pulsarSchema.getSchemaInfo().getSchema()));
        avroSchema.getFields().forEach(
                field -> builder.set(field.name(), rowRecordData.get(field.name()))
        );
        return builder.build();
    }
}