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
import org.junit.Test;

/**
 * The stream state checker test.
 */
public class StreamStateCheckerTest {
    @Test
    public void testStreamStateChecker() throws InterruptedException {
        StreamStateChecker streamStateChecker = new StreamStateChecker("stream01");
        streamStateChecker.addSendingOffset(1L);
        streamStateChecker.addSendingOffset(2L);
        streamStateChecker.addSendingOffset(3L);

        streamStateChecker.notifySendAll();

        streamStateChecker.updateAckOffset(1L);
        streamStateChecker.updateAckOffset(2L);
        streamStateChecker.updateAckOffset(3L);

        streamStateChecker.waitComplete();
    }

    @Test
    public void getMinUnAckOffset() {
        StreamStateChecker streamStateChecker = new StreamStateChecker("stream01");
        for (int i = 0; i < 10; i++) {
            streamStateChecker.addSendingOffset((long) i);
        }

        streamStateChecker.updateAckOffset(4L);
        streamStateChecker.updateAckOffset(0L);
        streamStateChecker.updateAckOffset(1L);
        streamStateChecker.updateAckOffset(5L);
        streamStateChecker.updateAckOffset(8L);

        assertEquals(2L, streamStateChecker.getMinUnAckOffset());

        streamStateChecker.updateAckOffset(2L);
        streamStateChecker.updateAckOffset(3L);
        assertEquals(6L, streamStateChecker.getMinUnAckOffset());
    }
}
