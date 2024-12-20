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
package org.apache.pulsar.client.internal;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class loads the implementation for {@link PulsarClientImplementationBinding}
 * and allows you to decouple the API from the actual implementation.
 * <b>This class is internal to the Pulsar API implementation, and it is not part of the public API
 * it is not meant to be used by client applications.</b>
 */
public class DefaultImplementation {
    private static final String DEFAULT_IMPLEMENTATION =
            "org.apache.pulsar.client.impl.PulsarClientImplementationBindingImpl";
    private static final Map<String, PulsarClientImplementationBinding> IMPLEMENTATIONS =
            new ConcurrentHashMap<>();

    /**
     * Access the actual implementation of the Pulsar Client API.
     *
     * @return the loaded implementation.
     */
    public static PulsarClientImplementationBinding getDefaultImplementation() {
        String clientImpl = System.getProperty("pulsar.client.implementation.class", DEFAULT_IMPLEMENTATION);
        return IMPLEMENTATIONS.computeIfAbsent(clientImpl, ci -> {
            PulsarClientImplementationBinding impl;
            try {
                impl = (PulsarClientImplementationBinding) ReflectionUtils
                        .newClassInstance(ci)
                        .getConstructor().newInstance();
            } catch (Throwable error) {
                throw new RuntimeException("Cannot load Pulsar Client Implementation: " + error, error);
            }
            return impl;
        });
    }
}
