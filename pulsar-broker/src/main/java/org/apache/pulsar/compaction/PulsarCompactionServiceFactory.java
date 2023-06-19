/*
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
package org.apache.pulsar.compaction;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;

public class PulsarCompactionServiceFactory implements CompactionServiceFactory {

    private PulsarService pulsarService;

    private Compactor compactor;

    public synchronized Compactor getCompactor() throws PulsarServerException {
        if (compactor == null) {
            compactor = newCompactor();
        }
        return compactor;
    }

    public Compactor getNullableCompactor() {
        return compactor;
    }

    protected Compactor newCompactor() throws PulsarServerException {
        return new TwoPhaseCompactor(pulsarService.getConfiguration(),
                pulsarService.getClient(), pulsarService.getBookKeeperClient(),
                pulsarService.getCompactorExecutor());
    }

    @Override
    public CompletableFuture<Void> initialize(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<TopicCompactionService> newTopicCompactionService(String topic) {
        PulsarTopicCompactionService pulsarTopicCompactionService =
                new PulsarTopicCompactionService(topic, pulsarService.getBookKeeperClient(), () -> {
                    try {
                        return this.getCompactor();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                });
        return CompletableFuture.completedFuture(pulsarTopicCompactionService);
    }

    @Override
    public void close() throws Exception {
        // noop
    }
}
