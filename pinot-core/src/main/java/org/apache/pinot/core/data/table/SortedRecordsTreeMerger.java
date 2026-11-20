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
package org.apache.pinot.core.data.table;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.spi.query.QueryThreadContext;


/**
 * Parallel tournament-tree merger for sorted GROUP BY records.
 *
 * <p>Each level pair-wise merges {@link SortedRecords} using {@link SortedRecordsMerger}. Independent
 * pairs run concurrently on {@code executorService}. {@code merger} is invoked concurrently and
 * must be thread-safe. A single input is returned as-is without copying. An empty list becomes an
 * empty {@link SortedRecords}.
 *
 * <p>{@link SortedRecordsMerger#mergeSortedRecordArray} allocates a new array and writes it back
 * onto the left wrapper, so left inputs at a given level can be passed through without a defensive
 * copy. Each input is used once per level.
 *
 * <p>This class is stateless and thread-safe.
 */
@ThreadSafe
public final class SortedRecordsTreeMerger {
  private SortedRecordsTreeMerger() {
  }

  /**
   * Merges {@code inputs} into one {@link SortedRecords}. Pair tasks are cancelled if the query
   * deadline {@code endTimeMs} is reached or a sibling pair fails.
   */
  public static SortedRecords mergeAll(List<SortedRecords> inputs, SortedRecordsMerger merger,
      ExecutorService executorService, long endTimeMs)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (inputs.isEmpty()) {
      return new SortedRecords(new Record[0], 0);
    }
    if (inputs.size() == 1) {
      return inputs.get(0);
    }
    List<SortedRecords> current = new ArrayList<>(inputs);
    while (current.size() > 1) {
      QueryThreadContext.checkTerminationAndSampleUsage("SortedRecordsTreeMerger");
      List<SortedRecords> next = new ArrayList<>((current.size() + 1) / 2);
      List<Future<SortedRecords>> futures = new ArrayList<>(current.size() / 2);
      try {
        for (int i = 0; i + 1 < current.size(); i += 2) {
          SortedRecords left = current.get(i);
          SortedRecords right = current.get(i + 1);
          futures.add(executorService.submit(() -> merger.mergeSortedRecordArray(left, right)));
        }
        if ((current.size() & 1) == 1) {
          next.add(current.get(current.size() - 1));
        }
        for (Future<SortedRecords> future : futures) {
          long waitTimeMs = endTimeMs - System.currentTimeMillis();
          if (waitTimeMs <= 0) {
            throw new TimeoutException("Timed out merging sorted GROUP BY records");
          }
          next.add(future.get(waitTimeMs, TimeUnit.MILLISECONDS));
        }
      } finally {
        for (Future<SortedRecords> future : futures) {
          future.cancel(true);
        }
      }
      current = next;
    }
    return current.get(0);
  }
}
