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
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.junit.Test;

/**
 * The source reader test.
 */
@Slf4j
public class SourceReaderTest {

    @Test
    public void testConvertToPulsarSchema() {
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
        Schema avroSchema = new Schema.Parser().parse(schema);

        GenericSchema<GenericRecord> pulsarSchema = SourceReader.convertToPulsarSchema(avroSchema, schema);

        assertEquals(avroSchema.getName(), pulsarSchema.getSchemaInfo().getName());
        assertEquals(avroSchema.getFields().size(), pulsarSchema.getFields().size());
        pulsarSchema.getFields().forEach(field -> {
            assertEquals(field.getName(), avroSchema.getField(field.getName()).name());
        });
    }
}
