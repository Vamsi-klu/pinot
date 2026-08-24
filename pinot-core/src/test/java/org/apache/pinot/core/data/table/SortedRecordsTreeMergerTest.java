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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


/**
 * Direct tests for {@link SortedRecordsTreeMerger} empty / one / odd-count inputs.
 */
public class SortedRecordsTreeMergerTest {
  private ExecutorService _executor;
  private SortedRecordsMerger _merger;

  @BeforeClass
  public void setUp() {
    _executor = Executors.newFixedThreadPool(2);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT intCol, COUNT(*) FROM testTable GROUP BY intCol ORDER BY intCol LIMIT 100");
    Comparator<Record> comparator = Comparator.comparing(record -> (Integer) record.getValues()[0]);
    _merger = new SortedRecordsMerger(queryContext, 100, comparator);
  }

  @AfterClass
  public void tearDown() {
    _executor.shutdownNow();
  }

  @Test
  public void testEmptyInput()
      throws Exception {
    SortedRecords merged = SortedRecordsTreeMerger.mergeAll(List.of(), _merger, _executor, Long.MAX_VALUE);
    assertEquals(merged._size, 0);
  }

  @Test
  public void testSingleInputReturnedAsIs()
      throws Exception {
    SortedRecords only = records(1, 2);
    assertSame(SortedRecordsTreeMerger.mergeAll(List.of(only), _merger, _executor, Long.MAX_VALUE), only);
  }

  @Test
  public void testOddCountCarriesLeftover()
      throws Exception {
    SortedRecords merged = SortedRecordsTreeMerger.mergeAll(
        List.of(records(0), records(2), records(1)), _merger, _executor, Long.MAX_VALUE);
    assertEquals(merged._size, 3);
    assertEquals(merged._records[0].getValues()[0], 0);
    assertEquals(merged._records[1].getValues()[0], 1);
    assertEquals(merged._records[2].getValues()[0], 2);
  }

  private static SortedRecords records(int... keys) {
    Record[] array = new Record[keys.length];
    for (int i = 0; i < keys.length; i++) {
      array[i] = new Record(new Object[]{keys[i], 1L});
    }
    return new SortedRecords(array, keys.length);
  }
}
