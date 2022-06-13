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
package org.apache.pulsar.client.api;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public class HealthCheckTest extends MockedPulsarServiceBaseTest {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        isTcpLookup = true;
        super.internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testHealthCheck() throws ExecutionException, InterruptedException {
        CompletableFuture<HealthCheckResult> healthCheckResultCompletableFuture = pulsarClient.healthCheck();
        HealthCheckResult healthCheckResult = healthCheckResultCompletableFuture.get();
        log.info("healthCheckResult: {}", healthCheckResult);
        Assert.assertTrue(healthCheckResult.isOk());
    }

    @Test
    public void testHealthCheckWithConcurrent() throws ExecutionException, InterruptedException {
        List<CompletableFuture<HealthCheckResult>> futureList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futureList.add(pulsarClient.healthCheck());
        }

        for (CompletableFuture<HealthCheckResult> completableFuture : futureList) {
            HealthCheckResult healthCheckResult = completableFuture.get();
            log.info("healthCheckResult: {}", healthCheckResult);
            Assert.assertTrue(healthCheckResult.isOk());
        }
    }
}
