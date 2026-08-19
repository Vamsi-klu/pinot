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
package org.apache.pinot.core.query.aggregation.function;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;


/**
 * Optional aggregation-function contract for the partitioned-aggregation path
 * (apache/pinot#12057).
 *
 * <p>When a table uses partitioned replica-group assignment, values of the partition column
 * are disjoint across partitions but may repeat across time segments of the same partition.
 * Combine must therefore:
 * <ol>
 *   <li>merge intermediate results <em>within</em> a partition (e.g. set union)</li>
 *   <li>extract a compact partition result (e.g. cardinality)</li>
 *   <li>merge those partition results <em>across</em> partitions (e.g. sum)</li>
 * </ol>
 *
 * <p>Implementations must be stateless and thread-safe, matching
 * {@link AggregationFunction}.
 *
 * @param <I> per-segment / within-partition intermediate type
 * @param <F> partition-level and final result type
 */
@ThreadSafe
public interface PartitionedAggregationFunction<I, F extends Comparable> extends AggregationFunction<I, F> {

  /**
   * Merges two intermediate results that belong to the same partition.
   */
  @Nullable
  I mergeWithinPartition(@Nullable I intermediateResult1, @Nullable I intermediateResult2);

  /**
   * Collapses a within-partition intermediate result to the compact partition result that can
   * be summed (or otherwise merged) across partitions.
   */
  @Nullable
  F extractPartitionResult(@Nullable I intermediateResult);

  /**
   * Merges two partition-level results (across partitions or across servers).
   */
  @Nullable
  F mergePartitionResults(@Nullable F partitionResult1, @Nullable F partitionResult2);
}
