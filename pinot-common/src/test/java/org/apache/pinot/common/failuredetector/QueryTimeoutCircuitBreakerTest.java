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
package org.apache.pinot.common.failuredetector;

import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * Unit tests for {@link QueryTimeoutCircuitBreaker}. Isolated from broker scatter-gather.
 */
public class QueryTimeoutCircuitBreakerTest {
  private static final String INSTANCE_ID = "Server_localhost_1234";
  private static final String HOST_NAME = "localhost";

  private RecordingFailureDetector _failureDetector;

  @BeforeMethod
  public void setUp() {
    _failureDetector = new RecordingFailureDetector();
  }

  @Test
  public void testDisabledDoesNotMarkUnhealthy() {
    QueryTimeoutCircuitBreaker breaker = new QueryTimeoutCircuitBreaker(_failureDetector, false, 1);
    assertFalse(breaker.isEnabled());
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    assertEquals(_failureDetector._unhealthy, List.of());
    assertEquals(breaker.getConsecutiveTimeouts(INSTANCE_ID), 0);
  }

  @Test
  public void testMarksUnhealthyAfterThresholdTimeouts() {
    QueryTimeoutCircuitBreaker breaker = new QueryTimeoutCircuitBreaker(_failureDetector, true, 3);
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    assertEquals(_failureDetector._unhealthy, List.of());
    assertEquals(breaker.getConsecutiveTimeouts(INSTANCE_ID), 2);

    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    assertEquals(_failureDetector._unhealthy, List.of(INSTANCE_ID));
    // Sentinel stays at the threshold until a real success (not a TCP-only probe).
    assertEquals(breaker.getConsecutiveTimeouts(INSTANCE_ID), 3);
  }

  @Test
  public void testSuccessClearsTimeoutStreak() {
    QueryTimeoutCircuitBreaker breaker = new QueryTimeoutCircuitBreaker(_failureDetector, true, 3);
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    breaker.recordSuccess(INSTANCE_ID);
    assertEquals(breaker.getConsecutiveTimeouts(INSTANCE_ID), 0);

    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    assertEquals(_failureDetector._unhealthy, List.of());
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    assertEquals(_failureDetector._unhealthy, List.of(INSTANCE_ID));
    breaker.recordSuccess(INSTANCE_ID);
    assertEquals(breaker.getConsecutiveTimeouts(INSTANCE_ID), 0);
  }

  @Test
  public void testTimeoutsAreTrackedPerInstance() {
    QueryTimeoutCircuitBreaker breaker = new QueryTimeoutCircuitBreaker(_failureDetector, true, 2);
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    breaker.recordTimeout("Server_other_5678", "other");
    assertEquals(_failureDetector._unhealthy, List.of());
    breaker.recordTimeout(INSTANCE_ID, HOST_NAME);
    assertEquals(_failureDetector._unhealthy, List.of(INSTANCE_ID));
    assertEquals(breaker.getConsecutiveTimeouts("Server_other_5678"), 1);
  }

  private static class RecordingFailureDetector extends NoOpFailureDetector {
    private final List<String> _unhealthy = new ArrayList<>();

    @Override
    public void markServerUnhealthy(String instanceId, String hostName) {
      _unhealthy.add(instanceId);
    }
  }
}
