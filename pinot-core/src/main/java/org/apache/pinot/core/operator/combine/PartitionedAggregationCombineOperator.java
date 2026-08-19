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
package org.apache.pinot.core.operator.combine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.combine.merger.PartitionedAggregationMerger;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Combine operator for aggregation-only queries that use the partitioned-aggregation path
 * (apache/pinot#12057).
 *
 * <p>Each worker attaches the producing segment's partition id (from the segment name) to the
 * results block. The main thread then runs {@link PartitionedAggregationMerger} and returns
 * final-result values so the broker can sum across servers with {@code mergeFinalResult}.
 */
@SuppressWarnings({"rawtypes"})
public class PartitionedAggregationCombineOperator extends BaseSingleBlockCombineOperator<AggregationResultsBlock> {
  private static final String EXPLAIN_NAME = "COMBINE_AGGREGATE_PARTITIONED";

  public PartitionedAggregationCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(null, operators, queryContext, executorService);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void processSegments() {
    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      AggregationResultsBlock resultsBlock;
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        resultsBlock = (AggregationResultsBlock) operator.nextBlock();
        IndexSegment segment = operator.getIndexSegment();
        if (segment != null) {
          resultsBlock.setPartitionId(SegmentUtils.getPartitionIdFromSegmentName(segment.getSegmentName()));
        }
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
      _blockingQueue.offer(resultsBlock);
    }
  }

  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {
    List<AggregationResultsBlock> blocks = new ArrayList<>(_numOperators);
    int numBlocksMerged = 0;
    long endTimeMs = _queryContext.getEndTimeMs();
    while (numBlocksMerged < _numOperators) {
      long waitTimeMs = endTimeMs - System.currentTimeMillis();
      if (waitTimeMs <= 0) {
        return getTimeoutResultsBlock(numBlocksMerged);
      }
      BaseResultsBlock blockToMerge = _blockingQueue.poll(waitTimeMs, TimeUnit.MILLISECONDS);
      if (blockToMerge == null) {
        return getTimeoutResultsBlock(numBlocksMerged);
      }
      if (blockToMerge.getErrorMessages() != null) {
        return blockToMerge;
      }
      blocks.add((AggregationResultsBlock) blockToMerge);
      numBlocksMerged++;
    }
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    List<Object> mergedResults = PartitionedAggregationMerger.merge(aggregationFunctions, blocks);
    // Partition results are already final (e.g. Integer cardinality). Tell serialization to use
    // final column types so the broker can sum them with mergeFinalResult.
    _queryContext.setServerReturnFinalResult(true);
    return new AggregationResultsBlock(aggregationFunctions, mergedResults, _queryContext);
  }
}
