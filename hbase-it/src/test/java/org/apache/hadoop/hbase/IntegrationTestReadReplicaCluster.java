/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.HConstants.HBASE_DIR;
import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_META_TABLE_SUFFIX;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.security.access.ReadOnlyController;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for validating the behavior of the Read-Replica feature. The test starts two separate
 * mini-clusters that share the same DFS and root directory. The active cluster has
 * {@value org.apache.hadoop.hbase.HConstants#HBASE_GLOBAL_READONLY_ENABLED_KEY} set to false, while
 * the replica cluster has this config variable set to true.
 */
@Category(IntegrationTests.class)
public class IntegrationTestReadReplicaCluster extends IntegrationTestBase {
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestReadReplicaCluster.class);
  protected static final String READ_ONLY_CONTROLLER_NAME = ReadOnlyController.class.getName();
  protected static final String META_TABLE_SUFFIX = "2";
  protected static final byte[] TABLE_NAME = Bytes.toBytes("testTable");
  protected static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf1");
  protected static final byte[] QUALIFIER = Bytes.toBytes("q1");
  protected Configuration activeConf;
  protected Configuration replicaConf;
  protected IntegrationTestingUtility activeUtil;
  protected IntegrationTestingUtility replicaUtil;
  SingleProcessHBaseCluster activeCluster;
  SingleProcessHBaseCluster replicaCluster;
  Connection activeConn;
  Connection replicaConn;
  Admin activeAdmin;
  Admin replicaAdmin;
  protected ProcedureExecutor<MasterProcedureEnv> replicaProcExecutor;
  Path rootDir;
  String activeClusterId;
  FileSystem fs;

  @Override
  public void setUpCluster() throws Exception {
    LOG.info("kevin: start setUpCluster");

    // Set up and start the active cluster
    util = new IntegrationTestingUtility();  // The test fails if util is not set
    activeUtil = util;
    activeConf = activeUtil.getConfiguration();
    activeConf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    activeConf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    activeConf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    activeConf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    // Minimize resource contention within the DFS
    activeConf.setInt("dfs.datanode.handler.count", 1);
    activeConf.setInt("dfs.namenode.handler.count", 1);
    LOG.info("kevin: starting cluster1 minicluster");
    activeCluster = activeUtil.startMiniCluster();
    String rootDir1 = activeCluster.getConfiguration().get(HBASE_DIR);
    LOG.info("kevin: finished starting cluster1 minicluster");
    activeClusterId = activeCluster.getMaster().getClusterId();
    LOG.info("kevin: active cluster's ID = {}", activeClusterId);
    activeConn = activeUtil.getConnection();
    activeAdmin = activeConn.getAdmin();

    // Use the active cluster's existing configuration to set up and start the replica cluster
    replicaConf = HBaseConfiguration.create(activeConf);
    replicaConf.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    replicaConf.set(HBASE_META_TABLE_SUFFIX, META_TABLE_SUFFIX);
    replicaUtil = new IntegrationTestingUtility(replicaConf);
    replicaUtil.setDataTestDirOnTestFS(activeUtil.getDataTestDirOnTestFS());
    replicaUtil.setDFSCluster(activeUtil.getDFSCluster());
    LOG.info("kevin: starting cluster2 minicluster");
    replicaCluster = replicaUtil.startMiniCluster();
    String rootDir2 = replicaCluster.getConfiguration().get(HBASE_DIR);
    LOG.info("kevin: finished starting cluster2 minicluster");
    LOG.info("kevin: replica cluster's ID = {}", replicaCluster.getMaster().getClusterId());
    replicaConn = replicaUtil.getConnection();
    replicaAdmin = replicaConn.getAdmin();
    replicaProcExecutor = replicaUtil.getHBaseCluster().getMaster().getMasterProcedureExecutor();

    fs = activeCluster.getMaster().getFileSystem();
    assertProperInitialization(rootDir1, rootDir2);

    LOG.info("kevin: dfs.datanode.handler.count = {}", activeConf.get("dfs.datanode.handler.count"));
    LOG.info("kevin: dfs.namenode.handler.count = {}", activeConf.get("dfs.namenode.handler.count"));

    LOG.info("kevin: end setUpCluster");
  }

  @Override
  public void cleanUpCluster() throws Exception {
    LOG.info("kevin: start cleanUpCluster");

    activeAdmin.close();
    replicaAdmin.close();
    activeConn.close();
    replicaConn.close();

    LOG.info("kevin: starting shutdownMiniHBaseCluster for cluster2");
    replicaUtil.shutdownMiniHBaseCluster();
    LOG.info("kevin: end of shutdownMiniHBaseCluster for cluster2");

    LOG.info("kevin: starting shutdownMiniZKCluster for cluster2");
    replicaUtil.shutdownMiniZKCluster();
    LOG.info("kevin: end of shutdownMiniZKCluster for cluster2");

    LOG.info("kevin: start restoring cluster1");
    activeUtil.restoreCluster();
    LOG.info("kevin: end restoring cluster1");

    LOG.info("kevin: end cleanUpCluster");
  }

  private void assertProperInitialization(String rootDir1, String rootDir2) throws IOException {
    assertEquals("The root directories of each cluster should be the same", rootDir1, rootDir2);
    rootDir = new Path(rootDir1);
    LOG.info("{} for both clusters is: {}", HBASE_DIR, rootDir);

    assertEquals("The data test directory should be the same for each cluster",
      activeUtil.getDataTestDirOnTestFS(), replicaUtil.getDataTestDirOnTestFS());
    LOG.info("dataTestDirOnTestFS for both clusters is: {}",
      activeUtil.getDataTestDirOnTestFS().toString());

    assertEquals("The two HBase clusters should be using the same DFS cluster",
      activeUtil.getDataTestDirOnTestFS(), replicaUtil.getDataTestDirOnTestFS());

    assertFalse(
      "The active cluster should have " + HBASE_GLOBAL_READONLY_ENABLED_KEY + " set to false",
      Boolean.parseBoolean(activeConf.get(HBASE_GLOBAL_READONLY_ENABLED_KEY)));
    assertTrue(
      "The replica cluster should have " + HBASE_GLOBAL_READONLY_ENABLED_KEY + " set to true",
      Boolean.parseBoolean(replicaConf.get(HBASE_GLOBAL_READONLY_ENABLED_KEY)));

    // Each cluster should have its own MasterData directory
    assertTrue("Expected " + MasterRegionFactory.MASTER_STORE_DIR + " to exist in the filesystem",
      fs.exists(new Path(rootDir, MasterRegionFactory.MASTER_STORE_DIR)));
    assertTrue(
      "Expected " + MasterRegionFactory.MASTER_STORE_DIR + "_" + META_TABLE_SUFFIX
        + " to exist in the filesystem",
      fs.exists(new Path(rootDir, MasterRegionFactory.MASTER_STORE_DIR + "_" + META_TABLE_SUFFIX)));
  }

  @Test
  public void testReadReplicaCluster() throws IOException, InterruptedException {
    // Create a table. The replica cluster won't have this table until refresh_meta is run.
    attemptCreateOnReplicaCluster();
    createTableOnActiveCluster();
      assertArrayEquals("The active cluster should have a table called " + Arrays.toString(TABLE_NAME),
              TABLE_NAME, Arrays.stream(activeConn.getAdmin().listTableNames()).toList().get(0).getName());
    assertTrue("The read replica cluster should not have any tables yet",
            Arrays.stream(replicaConn.getAdmin().listTableNames()).toList().isEmpty());
    refreshMeta();
    assertFalse("The replica cluster should now have a table called " + Arrays.toString(TABLE_NAME),
      Arrays.stream(replicaConn.getAdmin().listTableNames()).toList().isEmpty());

    // Add data to the active cluster
    byte[] row1 = Bytes.toBytes("row1");
    try (Table activeTable = activeConn.getTable(TableName.valueOf(TABLE_NAME))) {
      // Add data to the active cluster
      Put put = new Put(row1);
      put.addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes("1"));
      activeTable.put(put);
    }

    Result result = getRowFromActiveCluster(row1);
    assertFalse("The Get result should not be empty since data was inserted for row " + Arrays.toString(row1),
      result.isEmpty());
    activeAdmin.flush(TableName.valueOf(TABLE_NAME));

    result = getRowFromReplicaCluster(row1);
    assertTrue("The Get result should be empty since the replica cluster has not been refreshed",
      result.isEmpty());

    refreshMeta();
    refreshHFiles();

    result = getRowFromReplicaCluster(row1);
    assertFalse("The Get result should be not empty since the replica cluster has been refreshed",
      result.isEmpty());



    // TODO run create table and then refresh meta
    // What happens if i run refresh_meta and not refresh_hfiles
    // does the table apperar?
    // play around with doing on or the other







//    try (Table replicaTable = replicaConn.getTable(TableName.valueOf(TABLE_NAME))) {
//      Get get = new Get(row1);
//      get.addColumn(COLUMN_FAMILY, QUALIFIER);
//      Result result = replicaTable.get(get);
//      LOG.info("kevin: replica cluster get result = {}", result);
//      String s = "e";
//    }




    String s = "e";
  }

  void attemptCreateOnReplicaCluster() {
    // This createTable() should throw an exception and take us to the catch block
    try (Table ignored = replicaUtil.createTable(TableName.valueOf(TABLE_NAME), COLUMN_FAMILY)) {
      fail("An IOException should have occurred when trying to create a new table on the replica cluster");
    } catch (IOException e) {
      String expectedMsg = "Operation not allowed in Read-Only Mode";
      assertTrue("Expected an IOException with the following message: "
        + expectedMsg, e.getMessage().contains(expectedMsg));
    }
  }

  Table createTable(IntegrationTestingUtility util) throws IOException {
    try (Table table = util.createTable(TableName.valueOf(TABLE_NAME), COLUMN_FAMILY)) {
      return table;
    }
  }

  void createTableOnActiveCluster() throws IOException, InterruptedException {
    LOG.info("kevin: start createTableOnActiveCluster()");
    try (Table ignored = activeUtil.createTable(TableName.valueOf(TABLE_NAME), COLUMN_FAMILY)) {
      activeUtil.waitTableAvailable(TableName.valueOf(TABLE_NAME));
    }
    LOG.info("kevin: end createTableOnActiveCluster()");
  }

  void refreshMeta() throws IOException {
    LOG.info("kevin: start replicaAdmin.refreshMeta()");
    long prodId = replicaAdmin.refreshMeta();
    replicaUtil.waitForProcedureCompletion(prodId, replicaProcExecutor, 1000);
    LOG.info("kevin: end replicaAdmin.refreshMeta()");
  }

  void refreshHFiles() throws IOException {
    LOG.info("kevin: start replicaAdmin.refreshHFiles()");
    long prodId = replicaAdmin.refreshHFiles();
    replicaUtil.waitForProcedureCompletion(prodId, replicaProcExecutor, 1000);
    LOG.info("kevin: end replicaAdmin.refreshHFiles()");
  }

  Result getRow(Connection conn, byte[] row) throws IOException {
    try (Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {
      // Verify no data was added to the replica cluster
      Get get = new Get(row);
      get.addColumn(COLUMN_FAMILY, QUALIFIER);
      return table.get(get);
    }
  }

  Result getRowFromActiveCluster(byte[] row) throws IOException {
    return getRow(activeConn, row);
  }

  Result getRowFromReplicaCluster(byte[] row) throws IOException {
    return getRow(replicaConn, row);
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    return 0;
  }

  @Override
  public TableName getTablename() {
    return null;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return Set.of();
  }

  @Override
  public void setUpMonkey() {
    LOG.info("Skipping setup of Chaos Monkey");
  }

  @Override
  public void cleanUpMonkey() {
    LOG.info("Skipping cleanup of Chaos Monkey because it was never set up");
  }
}
