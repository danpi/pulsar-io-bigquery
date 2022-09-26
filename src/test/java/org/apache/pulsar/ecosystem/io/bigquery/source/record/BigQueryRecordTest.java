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

import static org.junit.Assert.assertEquals;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.ecosystem.io.bigquery.source.reader.SourceReader;
import org.junit.Test;

/**
 * The bigquery record test.
 */
public class BigQueryRecordTest {
    String schema = "{\n"
            + "\"type\": \"record\",\n"
            + "\"name\": \"__root__\","
            + "\"fields\": [\n"
            + "        {\n"
            + "            \"name\": \"id\",\n"
            + "            \"type\": [\n"
            + "                \"null\",\n"
            + "                \"long\"\n"
            + "            ]\n"
            + "        },\n"
            + "        {\n"
            + "            \"name\": \"message\",\n"
            + "            \"type\": [\n"
            + "                \"null\",\n"
            + "                \"string\"\n"
            + "            ]\n"
            + "        }\n"
            + "    ]\n"
            + "}\n";

    @Test
    public void testGetGenericRecord() {
        Schema avroSchema = new Schema.Parser().parse(schema);
        GenericSchema<GenericRecord> pulsarSchema = SourceReader.convertToPulsarSchema(avroSchema, schema);

        org.apache.avro.generic.GenericRecord rowGenericRecord =
                new GenericRecordBuilder(avroSchema).set("id", 10).set("message", "message10").build();
        BigQueryRecord.setPulsarSchema(pulsarSchema);
        GenericRecord genericRecord = BigQueryRecord.getGenericRecord(rowGenericRecord);
        assertEquals(10, genericRecord.getField("id"));
        assertEquals("message10", genericRecord.getField("message"));
    }
}
