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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Correctness tests for partition-aware DISTINCTCOUNT combine (apache/pinot#12057).
 */
public class PartitionedAggregationMergerTest {

  @Test
  public void testDistinctCountUnionsWithinPartitionAndSumsAcrossPartitions()
      throws Exception {
    QueryContext queryContext = partitionedQuery();
    assertTrue(queryContext.isEnablePartitionedAggregation());
    assertTrue(queryContext.isServerReturnFinalResult());
    AggregationFunction[] functions = queryContext.getAggregationFunctions();

    // Partition 0, two time segments sharing userId u1 — must union, not sum.
    AggregationResultsBlock p0s1 = block(functions, queryContext, 0, setOf("u0", "u1"));
    AggregationResultsBlock p0s2 = block(functions, queryContext, 0, setOf("u1", "u2"));
    // Partition 1 is disjoint.
    AggregationResultsBlock p1s1 = block(functions, queryContext, 1, setOf("u3", "u4"));
    // Capture before merge — mergeWithinPartition mutates the first set in place.
    int naiveSum = ((Set<?>) p0s1.getResults().get(0)).size() + ((Set<?>) p0s2.getResults().get(0)).size()
        + ((Set<?>) p1s1.getResults().get(0)).size();
    assertEquals(naiveSum, 6);

    List<Object> merged = PartitionedAggregationMerger.merge(functions, List.of(p0s1, p0s2, p1s1));
    assertEquals(merged.size(), 1);
    assertEquals(merged.get(0), 5);

    AggregationResultsBlock finalBlock = new AggregationResultsBlock(functions, merged, queryContext);
    finalBlock.setResultsAreFinal(true);
    DataTable dataTable = finalBlock.getDataTable();
    assertEquals(dataTable.getInt(0, 0), 5);
  }

  @Test
  public void testMissingPartitionIdIsRejected() {
    QueryContext queryContext = partitionedQuery();
    AggregationFunction[] functions = queryContext.getAggregationFunctions();

    AggregationResultsBlock a = block(functions, queryContext, null, setOf("a", "b"));
    AggregationResultsBlock b = block(functions, queryContext, null, setOf("b", "c"));
    assertThrows(IllegalStateException.class, () -> PartitionedAggregationMerger.merge(functions, List.of(a, b)));
  }

  @Test
  public void testNullIntermediateInSamePartitionIsSkipped() {
    QueryContext queryContext = partitionedQuery();
    AggregationFunction[] functions = queryContext.getAggregationFunctions();

    List<Object> nullResults = new ArrayList<>();
    nullResults.add(null);
    AggregationResultsBlock empty = new AggregationResultsBlock(functions, nullResults, queryContext);
    empty.setPartitionId(0);
    AggregationResultsBlock values = block(functions, queryContext, 0, setOf("a", "b"));
    List<Object> merged = PartitionedAggregationMerger.merge(functions, List.of(empty, values));
    assertEquals(merged.get(0), 2);
  }

  private static QueryContext partitionedQuery() {
    return QueryContextConverterUtils.getQueryContext(
        "SET enablePartitionedAggregation = true; SELECT DISTINCTCOUNT(userId) FROM test");
  }

  private static AggregationResultsBlock block(AggregationFunction[] functions, QueryContext queryContext,
      Integer partitionId, Set<String> values) {
    AggregationResultsBlock block = new AggregationResultsBlock(functions, List.of(values), queryContext);
    block.setPartitionId(partitionId);
    return block;
  }

  private static Set<String> setOf(String... values) {
    return new HashSet<>(List.of(values));
  }
}
