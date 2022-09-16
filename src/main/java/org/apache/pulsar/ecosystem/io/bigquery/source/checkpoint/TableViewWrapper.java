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

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.io.core.SourceContext;

/**
 * The wrapper class for pulsar internal topic.
 */
public class TableViewWrapper {
    private static final String PULSAR_INTERNAL_TOPIC = "__internal_topic__";
    private SourceContext sourceContext;
    private PulsarClient pulsarClient;
    private Producer<byte[]> producer;
    private TableView<byte[]> tv;

    public TableViewWrapper(SourceContext sourceContext) throws Exception {
        this.sourceContext = sourceContext;
        buildPulsarClient();
        this.producer =
                pulsarClient.newProducer(Schema.BYTES).topic(sourceContext.getSourceName() + PULSAR_INTERNAL_TOPIC)
                        .create();
        this.tv = pulsarClient.newTableViewBuilder(Schema.BYTES)
                .topic(sourceContext.getSourceName() + PULSAR_INTERNAL_TOPIC).create();
    }

    private void buildPulsarClient() {
        if (pulsarClient == null) {
            pulsarClient = sourceContext.getPulsarClient();
        }
    }

    public void writeMessage(String key, byte[] msgBody) throws PulsarClientException {
        producer.newMessage().value(msgBody).key(key).send();
    }

    public byte[] getMessageByTableView(String key) {
        return tv.get(key);
    }

}