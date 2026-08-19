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
package org.apache.pinot.core.operator.combine.merger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.PartitionedAggregationFunction;


/**
 * Combines per-segment aggregation blocks using the partitioned-aggregation contract
 * (apache/pinot#12057).
 *
 * <p>Blocks with the same {@link AggregationResultsBlock#getPartitionId()} are merged with
 * {@link PartitionedAggregationFunction#mergeWithinPartition}. Each partition is then collapsed
 * with {@link PartitionedAggregationFunction#extractPartitionResult} and those results are merged
 * with {@link PartitionedAggregationFunction#mergePartitionResults}.
 *
 * <p>A missing partition id is treated as its own partition so the path degrades to regular
 * per-segment merge (then extract) instead of incorrectly summing across an unknown grouping.
 *
 * <p>Stateless and thread-safe.
 */
@ThreadSafe
@SuppressWarnings({"rawtypes", "unchecked"})
public final class PartitionedAggregationMerger {
  private PartitionedAggregationMerger() {
  }

  /**
   * Returns one final result per aggregation function after partition-aware combine.
   */
  public static List<Object> merge(AggregationFunction[] aggregationFunctions,
      List<AggregationResultsBlock> blocks) {
    int numFunctions = aggregationFunctions.length;
    Map<Integer, Object[]> perPartition = new HashMap<>();
    int nextSyntheticPartition = -1;
    for (AggregationResultsBlock block : blocks) {
      if (block.getNumRows() == 0) {
        continue;
      }
      List<Object> results = block.getResults();
      if (results == null || results.isEmpty()) {
        continue;
      }
      Integer partitionId = block.getPartitionId();
      if (partitionId == null) {
        partitionId = nextSyntheticPartition--;
      }
      Object[] partitionState = perPartition.computeIfAbsent(partitionId, id -> new Object[numFunctions]);
      for (int i = 0; i < numFunctions; i++) {
        AggregationFunction function = aggregationFunctions[i];
        Object incoming = results.get(i);
        if (partitionState[i] == null) {
          partitionState[i] = incoming;
        } else if (function instanceof PartitionedAggregationFunction) {
          partitionState[i] = ((PartitionedAggregationFunction) function).mergeWithinPartition(partitionState[i],
              incoming);
        } else {
          partitionState[i] = function.merge(partitionState[i], incoming);
        }
      }
    }

    List<Object> merged = new ArrayList<>(numFunctions);
    for (int i = 0; i < numFunctions; i++) {
      AggregationFunction function = aggregationFunctions[i];
      Object partitionMerged = null;
      for (Object[] partitionState : perPartition.values()) {
        Object partitionResult;
        if (function instanceof PartitionedAggregationFunction) {
          partitionResult = ((PartitionedAggregationFunction) function).extractPartitionResult(partitionState[i]);
          partitionMerged =
              ((PartitionedAggregationFunction) function).mergePartitionResults(partitionMerged, partitionResult);
        } else {
          partitionResult = function.extractFinalResult(partitionState[i]);
          partitionMerged = partitionMerged == null ? partitionResult
              : function.mergeFinalResult(partitionMerged, partitionResult);
        }
      }
      if (partitionMerged == null && function instanceof PartitionedAggregationFunction) {
        partitionMerged = ((PartitionedAggregationFunction) function).extractPartitionResult(null);
      }
      merged.add(partitionMerged);
    }
    return merged;
  }
}
