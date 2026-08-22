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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks consecutive query timeouts per server and marks the server unhealthy through
 * {@link FailureDetector} once a configurable threshold is reached.
 *
 * <p>This is the timeout trigger for the broker circuit breaker described in apache/pinot#8618.
 * Connection-level failures continue to call {@code FailureDetector.markServerUnhealthy} directly.
 * This helper is opt-in (disabled by default) so mixed-version clusters keep today's
 * connection-only exclusion behavior.
 *
 * <p>{@link #recordSuccess} is the only reset. After a trip the counter stays at the threshold
 * so a TCP-only half-open probe from {@link FailureDetector} cannot clear the streak; the next
 * timeout re-marks immediately until a real query succeeds.
 *
 * <p>Thread-safe. One instance is shared by all scatter-gather paths on a broker.
 */
@ThreadSafe
public class QueryTimeoutCircuitBreaker {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryTimeoutCircuitBreaker.class);

  private final FailureDetector _failureDetector;
  private final boolean _enabled;
  private final int _threshold;
  private final ConcurrentHashMap<String, AtomicInteger> _consecutiveTimeouts = new ConcurrentHashMap<>();

  public QueryTimeoutCircuitBreaker(FailureDetector failureDetector, PinotConfiguration config) {
    this(failureDetector, config.getProperty(CommonConstants.Broker.FailureDetector.CONFIG_OF_MARK_UNHEALTHY_ON_TIMEOUT,
            CommonConstants.Broker.FailureDetector.DEFAULT_MARK_UNHEALTHY_ON_TIMEOUT),
        config.getProperty(CommonConstants.Broker.FailureDetector.CONFIG_OF_TIMEOUT_FAILURE_THRESHOLD,
            CommonConstants.Broker.FailureDetector.DEFAULT_TIMEOUT_FAILURE_THRESHOLD));
  }

  @VisibleForTesting
  QueryTimeoutCircuitBreaker(FailureDetector failureDetector, boolean enabled, int threshold) {
    _failureDetector = failureDetector;
    _enabled = enabled;
    _threshold = threshold < 1 ? 1 : threshold;
    if (enabled && threshold < 1) {
      LOGGER.warn("Invalid timeout failure threshold {}, using 1", threshold);
    }
    if (enabled && failureDetector instanceof NoOpFailureDetector) {
      LOGGER.warn("Query timeout circuit breaker is enabled but failure detector is NO_OP; "
          + "servers will not be excluded from routing");
    }
    LOGGER.info("QueryTimeoutCircuitBreaker enabled={}, threshold={}, detector={}", _enabled, _threshold,
        failureDetector.getClass().getSimpleName());
  }

  public boolean isEnabled() {
    return _enabled;
  }

  /**
   * Records a successful response from {@code instanceId} and clears its timeout streak.
   */
  public void recordSuccess(String instanceId) {
    if (!_enabled) {
      return;
    }
    _consecutiveTimeouts.remove(instanceId);
  }

  /**
   * Records that {@code instanceId} did not respond before the query deadline. Marks the server
   * unhealthy when the consecutive-timeout threshold is reached.
   */
  public void recordTimeout(String instanceId, @Nullable String hostName) {
    if (!_enabled) {
      return;
    }
    AtomicInteger after = _consecutiveTimeouts.compute(instanceId, (id, count) -> {
      int next = (count == null ? 0 : count.get()) + 1;
      return new AtomicInteger(next);
    });
    if (after.get() >= _threshold && _consecutiveTimeouts.remove(instanceId, after)) {
      LOGGER.warn("Marking server {} unhealthy after {} consecutive query timeouts", instanceId, after.get());
      _failureDetector.markServerUnhealthy(instanceId, hostName);
      // Keep a sentinel at the threshold so a TCP-only retry cannot fully reset the streak.
      _consecutiveTimeouts.putIfAbsent(instanceId, new AtomicInteger(_threshold));
    }
  }

  @VisibleForTesting
  int getConsecutiveTimeouts(String instanceId) {
    AtomicInteger count = _consecutiveTimeouts.get(instanceId);
    return count == null ? 0 : count.get();
  }
}
