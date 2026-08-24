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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.combine.merger.PartitionedAggregationMerger;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;


/**
 * Combine operator for aggregation-only queries that use the partitioned-aggregation path
 * (apache/pinot#12057).
 *
 * <p>Each worker attaches the producing segment's partition id (from the segment name or column
 * partition metadata) to the results block. The main thread then runs
 * {@link PartitionedAggregationMerger}. Results are already final cardinalities; the query option
 * sets {@code serverReturnFinalResult} in {@link QueryContext} so broker and server agree.
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
        Integer partitionId = resolvePartitionId(operator, _queryContext);
        if (partitionId == null) {
          throw new IllegalStateException(
              "enablePartitionedAggregation requires a resolvable partition id on every segment. "
                  + "Use partitioned replica-group assignment or LLC / uploaded-realtime segment names.");
        }
        resultsBlock.setPartitionId(partitionId);
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
    AggregationResultsBlock merged = new AggregationResultsBlock(aggregationFunctions, mergedResults, _queryContext);
    merged.setResultsAreFinal(true);
    return merged;
  }

  @Nullable
  static Integer resolvePartitionId(Operator operator, QueryContext queryContext) {
    IndexSegment segment = findSegment(operator);
    if (segment == null) {
      return null;
    }
    Integer fromName = SegmentUtils.getPartitionIdFromSegmentName(segment.getSegmentName());
    if (fromName != null) {
      return fromName;
    }
    return partitionIdFromColumnMetadata(segment, queryContext);
  }

  @Nullable
  private static Integer partitionIdFromColumnMetadata(IndexSegment segment, QueryContext queryContext) {
    SegmentMetadata metadata = segment.getSegmentMetadata();
    AggregationFunction[] functions = queryContext.getAggregationFunctions();
    if (metadata == null || functions == null) {
      return null;
    }
    Integer found = null;
    for (AggregationFunction function : functions) {
      for (Object input : function.getInputExpressions()) {
        if (!(input instanceof ExpressionContext expression)
            || expression.getType() != ExpressionContext.Type.IDENTIFIER) {
          continue;
        }
        ColumnMetadata columnMetadata = metadata.getColumnMetadataFor(expression.getIdentifier());
        if (columnMetadata == null) {
          continue;
        }
        Set<Integer> partitions = columnMetadata.getPartitions();
        if (partitions == null || partitions.size() != 1) {
          continue;
        }
        int partitionId = partitions.iterator().next();
        if (found != null && found != partitionId) {
          return null;
        }
        found = partitionId;
      }
    }
    return found;
  }

  @Nullable
  private static IndexSegment findSegment(Operator operator) {
    IndexSegment segment = operator.getIndexSegment();
    if (segment != null) {
      return segment;
    }
    List<? extends Operator> children = operator.getChildOperators();
    if (children == null) {
      return null;
    }
    for (Operator child : children) {
      segment = findSegment(child);
      if (segment != null) {
        return segment;
      }
    }
    return null;
  }
}
