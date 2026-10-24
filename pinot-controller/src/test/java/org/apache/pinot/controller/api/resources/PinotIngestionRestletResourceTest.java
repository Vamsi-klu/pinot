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
package org.apache.pinot.controller.api.resources;

import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Guard tests for file/URI ingest table eligibility (apache/pinot#11914).
 */
public class PinotIngestionRestletResourceTest {

  @Test
  public void testAllowsNonUpsertRealtime() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("events").build();
    PinotIngestionRestletResource.validateFileIngestTable(tableConfig, "events_REALTIME");
  }

  @Test
  public void testRejectsUpsertRealtime() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("events")
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .build();
    IllegalStateException error = expectIllegalState(tableConfig, "events_REALTIME");
    assertTrue(error.getMessage().contains("upsert table"));
  }

  @Test
  public void testRejectsDedupRealtime() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("events")
        .setDedupConfig(new DedupConfig())
        .build();
    IllegalStateException error = expectIllegalState(tableConfig, "events_REALTIME");
    assertTrue(error.getMessage().contains("dedup table"));
  }

  private static IllegalStateException expectIllegalState(TableConfig tableConfig, String tableNameWithType) {
    try {
      PinotIngestionRestletResource.validateFileIngestTable(tableConfig, tableNameWithType);
    } catch (IllegalStateException e) {
      return e;
    }
    throw new AssertionError("expected IllegalStateException");
  }
}
