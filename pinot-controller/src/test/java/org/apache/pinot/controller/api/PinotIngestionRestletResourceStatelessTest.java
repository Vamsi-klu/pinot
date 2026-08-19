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
package org.apache.pinot.controller.api;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the ingestion restlet
 *
 */
@Test(groups = "stateless")
public class PinotIngestionRestletResourceStatelessTest extends ControllerTest {
  private static final String TABLE_NAME = "testTable";
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private static final String REALTIME_TABLE_NAME = "testRealtimeTable";
  private static final String REALTIME_TABLE_NAME_WITH_TYPE = "testRealtimeTable_REALTIME";
  private File _inputFile;
  private File _realtimeInputFile;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    Map<String, Object> controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.put(ControllerConf.INGEST_FROM_URI_ALLOW_LOCAL_FILE_SYSTEM, true);
    startController(controllerConfig);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);

    // Add schema & table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension("breed", FieldSpec.DataType.STRING)
            .addSingleValueDimension("name", FieldSpec.DataType.STRING).build();
    _helixResourceManager.addSchema(schema, true, false);
    _helixResourceManager.addTable(tableConfig);

    Schema realtimeSchema = new Schema.SchemaBuilder().setSchemaName(REALTIME_TABLE_NAME)
        .addSingleValueDimension("breed", FieldSpec.DataType.STRING)
        .addSingleValueDimension("name", FieldSpec.DataType.STRING)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    TableConfig realtimeTableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(REALTIME_TABLE_NAME)
        .setTimeColumnName("ts")
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap())
        .build();
    _helixResourceManager.addSchema(realtimeSchema, true, false);
    _helixResourceManager.addTable(realtimeTableConfig);

    // Create a file with few records
    _inputFile = new File(FileUtils.getTempDirectory(), "pinotIngestionRestletResourceTest_data.csv");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(_inputFile))) {
      bw.write("breed|name\n");
      bw.write("dog|cooper\n");
      bw.write("cat|kylo\n");
      bw.write("dog|cookie\n");
    }

    _realtimeInputFile = new File(FileUtils.getTempDirectory(), "pinotIngestionRestletResourceTest_realtime.csv");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(_realtimeInputFile))) {
      bw.write("breed|name|ts\n");
      bw.write("dog|cooper|1000\n");
      bw.write("cat|kylo|2000\n");
      bw.write("dog|cookie|3000\n");
    }
  }

  @Test
  public void testIngestEndpoint()
      throws Exception {

    PinotAdminClient adminClient = getOrCreateAdminClient();
    List<String> segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 0);

    // the ingestion dir does not exist before ingesting files
    File ingestionDir = new File(_controllerConfig.getLocalTempDir() + "/ingestion_dir");
    assertFalse(ingestionDir.exists());

    // ingest from file
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "csv");
    batchConfigMap.put(String.format("%s.delimiter", BatchConfigProperties.RECORD_READER_PROP_PREFIX), "|");
    assertEquals(adminClient.getFileIngestClient().ingestFromFile(TABLE_NAME_WITH_TYPE, batchConfigMap, _inputFile),
        200);
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 1);

    // ingest from URI
    assertEquals(adminClient.getFileIngestClient()
            .ingestFromUri(TABLE_NAME_WITH_TYPE, batchConfigMap,
                String.format("file://%s", _inputFile.getAbsolutePath()), _inputFile),
        200);
    segments = _helixResourceManager.getSegmentsFor(TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 2);

    // the ingestion dir exists after ingesting files. We check the existence to make sure this dir is created under
    // _controllerConfig.getLocalTempDir()
    assertTrue(ingestionDir.exists());
  }

  @Test
  public void testIngestEndpointRealtimeTable()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    List<String> segments = _helixResourceManager.getSegmentsFor(REALTIME_TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 0);

    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FORMAT, "csv");
    batchConfigMap.put(String.format("%s.delimiter", BatchConfigProperties.RECORD_READER_PROP_PREFIX), "|");
    assertEquals(
        adminClient.getFileIngestClient()
            .ingestFromFile(REALTIME_TABLE_NAME_WITH_TYPE, batchConfigMap, _realtimeInputFile),
        200);
    segments = _helixResourceManager.getSegmentsFor(REALTIME_TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 1);

    assertEquals(adminClient.getFileIngestClient()
            .ingestFromUri(REALTIME_TABLE_NAME_WITH_TYPE, batchConfigMap,
                String.format("file://%s", _realtimeInputFile.getAbsolutePath()), _realtimeInputFile),
        200);
    segments = _helixResourceManager.getSegmentsFor(REALTIME_TABLE_NAME_WITH_TYPE, false);
    assertEquals(segments.size(), 2);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(_inputFile);
    FileUtils.deleteQuietly(_realtimeInputFile);
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
