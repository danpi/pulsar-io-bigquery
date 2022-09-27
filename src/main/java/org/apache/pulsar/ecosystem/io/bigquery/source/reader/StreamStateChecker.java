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

import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/**
 * The state checker for a stream.
 */
@Slf4j
public class StreamStateChecker {
    private final long maxTimeWaitMs = 1000;
    private TreeSet<Long> unAckOffset = new TreeSet<>();
    private Lock unAckOffsetLock = new ReentrantLock();
    private Object object = new Object();
    private boolean isSendAll = false;
    private String stream;

    public StreamStateChecker(String stream) {
        this.stream = stream;
    }

    public void addSendingOffset(Long offset) {
        try {
            unAckOffsetLock.lock();
            unAckOffset.add(offset);
        } finally {
            unAckOffsetLock.unlock();
        }
    }

    public void updateAckOffset(Long offset) {
        try {
            unAckOffsetLock.lock();
            unAckOffset.remove(offset);
        } finally {
            unAckOffsetLock.unlock();
        }
        if (isSendAll && unAckOffset.isEmpty()) {
            synchronized (object) {
                object.notify();
            }
        }
    }

    public long getMinUnAckOffset() {
        long minUnAckOffset = 0;
        try {
            unAckOffsetLock.lock();
            minUnAckOffset = unAckOffset.first();
        } finally {
            unAckOffsetLock.unlock();
        }
        return minUnAckOffset;
    }

    public void notifySendAll() {
        this.isSendAll = true;
        if (isSendAll && unAckOffset.isEmpty()) {
            synchronized (object) {
                object.notify();
            }
        }
    }

    public void waitComplete() {
        while (!isStateDone()) {
            synchronized (object) {
                try {
                    object.wait(maxTimeWaitMs);
                    log.info("wait signal timeout={}", stream);
                } catch (Exception e) {
                    log.error("wait meet exception={}", stream, e);
                }
            }
        }
    }

    private boolean isStateDone() {
        return isSendAll && unAckOffset.isEmpty();
    }
}
