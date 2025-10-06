package org.apache.hadoop.hbase.readreplica;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ActiveClusterSuffix;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.security.access.ReadOnlyController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.HBASE_DIR;
import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_META_TABLE_SUFFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class IntegrationTestReadReplicaBase extends IntegrationTestBase {
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestReadReplicaBase.class);
  protected static final String READ_ONLY_CONTROLLER_NAME = ReadOnlyController.class.getName();
  protected static final String TEST_META_TABLE_SUFFIX = "2";
  // protected static final byte[] TABLE_NAME = Bytes.toBytes("testTable");
  protected static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf1");
  protected static final byte[] QUALIFIER = Bytes.toBytes("q1");
  SingleProcessHBaseCluster clusterA;
  SingleProcessHBaseCluster clusterB;
  protected IntegrationTestingUtility utilA;
  protected IntegrationTestingUtility utilB;
  protected Configuration confA;
  protected Configuration confB;
  Connection connectionA;
  Connection connectionB;
  Path rootDir;
  FileSystem fs;

  @Override
  public void setUpCluster() throws Exception {
    LOG.info("kevin: start setUpCluster");

    // Set up and start the active cluster
    util = new IntegrationTestingUtility(); // The test fails if util is not set
    utilA = util;
    confA = utilA.getConfiguration();
    confA.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    confA.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    confA.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    confA.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    // Minimize resource contention within the DFS
    confA.setInt("dfs.datanode.handler.count", 1);
    confA.setInt("dfs.namenode.handler.count", 1);
    // Prevent retries for Puts on the replica cluster that are expected to fail
    confA.setInt(HBASE_CLIENT_RETRIES_NUMBER, 0);
    // activeConf.setInt("dfs.socket.timeout", 180*1000);
    // activeConf.setInt("dfs.datanode.socket.write.timeout", 180*1000);
    // activeConf.setInt("dfs.client-write-packet-timeout", 120*1000);

    LOG.info("kevin: starting clusterA minicluster");
    clusterA = utilA.startMiniCluster();
    String rootDir1 = clusterA.getConfiguration().get(HBASE_DIR);
    LOG.info("kevin: finished starting clusterA minicluster");
    connectionA = utilA.getConnection();

    // Use the active cluster's existing configuration to set up and start the replica cluster
    confB = HBaseConfiguration.create(confA);
    confB.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    confB.set(HBASE_META_TABLE_SUFFIX, TEST_META_TABLE_SUFFIX);
    utilB = new IntegrationTestingUtility(confB);
    utilB.setDataTestDirOnTestFS(utilA.getDataTestDirOnTestFS());
    utilB.setDFSCluster(utilA.getDFSCluster());
    LOG.info("kevin: starting clusterB minicluster");
    clusterB = utilB.startMiniCluster();
    String rootDir2 = clusterB.getConfiguration().get(HBASE_DIR);
    LOG.info("kevin: finished starting clusterB minicluster");
    connectionB = utilB.getConnection();

    LOG.info("kevin: active cluster ID = {}", clusterA.getMaster().getClusterId());
    LOG.info("kevin: replica cluster ID = {}", clusterB.getMaster().getClusterId());
    // assertNotEquals(activeCluster.getMaster().getClusterId(),
    // replicaCluster.getMaster().getClusterId());

    fs = clusterA.getMaster().getFileSystem();
    assertProperInitialization(rootDir1, rootDir2);

    LOG.info("kevin: dfs.datanode.handler.count = {}",
      confA.get("dfs.datanode.handler.count"));
    LOG.info("kevin: dfs.namenode.handler.count = {}",
      confA.get("dfs.namenode.handler.count"));

    LOG.info("kevin: end setUpCluster");
  }

  @Override
  public void cleanUpCluster() throws Exception {
    LOG.info("kevin: start cleanUpCluster");

    connectionA.close();
    connectionB.close();

    LOG.info("kevin: starting shutdownMiniHBaseCluster for clusterB");
    utilB.shutdownMiniHBaseCluster();
    LOG.info("kevin: end of shutdownMiniHBaseCluster for clusterB");

    LOG.info("kevin: starting shutdownMiniZKCluster for clusterB");
    utilB.shutdownMiniZKCluster();
    LOG.info("kevin: end of shutdownMiniZKCluster for clusterB");

    LOG.info("kevin: start restoring clusterA");
    utilA.restoreCluster();
    LOG.info("kevin: end restoring clusterA");

    LOG.info("kevin: end cleanUpCluster");
  }

  private void assertProperInitialization(String rootDir1, String rootDir2) throws IOException {
    assertEquals("The root directories of each cluster should be the same", rootDir1, rootDir2);
    rootDir = new Path(rootDir1);
    LOG.info("{} for both clusters is: {}", HBASE_DIR, rootDir);

    assertEquals("The data test directory should be the same for each cluster",
      utilA.getDataTestDirOnTestFS(), utilB.getDataTestDirOnTestFS());
    LOG.info("dataTestDirOnTestFS for both clusters is: {}",
      utilA.getDataTestDirOnTestFS().toString());

    assertEquals("The two HBase clusters should be using the same DFS cluster",
      utilA.getDataTestDirOnTestFS(), utilB.getDataTestDirOnTestFS());

    assertFalse(
      "The active cluster should have " + HBASE_GLOBAL_READONLY_ENABLED_KEY + " set to false",
      Boolean.parseBoolean(confA.get(HBASE_GLOBAL_READONLY_ENABLED_KEY)));
    assertTrue(
      "The replica cluster should have " + HBASE_GLOBAL_READONLY_ENABLED_KEY + " set to true",
      Boolean.parseBoolean(confB.get(HBASE_GLOBAL_READONLY_ENABLED_KEY)));

    // Each cluster should have its own MasterData directory
    assertTrue("Expected " + MasterRegionFactory.MASTER_STORE_DIR + " to exist in the filesystem",
      fs.exists(new Path(rootDir, MasterRegionFactory.MASTER_STORE_DIR)));
    assertTrue(
      "Expected " + MasterRegionFactory.MASTER_STORE_DIR + "_" + TEST_META_TABLE_SUFFIX
        + " to exist in the filesystem",
      fs.exists(
        new Path(rootDir, MasterRegionFactory.MASTER_STORE_DIR + "_" + TEST_META_TABLE_SUFFIX)));
  }

  boolean isReplicaClusterUtil(IntegrationTestingUtility util) {
    return Boolean.parseBoolean(util.getConfiguration().get(HBASE_GLOBAL_READONLY_ENABLED_KEY));
  }

  void assertIsReplicaClusterUtil(IntegrationTestingUtility util) {
    assertTrue(
      "Expected " + IntegrationTestingUtility.class.getName() + " object from a replica cluster",
      isReplicaClusterUtil(util));
  }

  void assertIsActiveClusterUtil(IntegrationTestingUtility util) {
    assertFalse(
      "Expected " + IntegrationTestingUtility.class.getName() + " object from an active cluster",
      isReplicaClusterUtil(util));
  }

  void attemptCreateOnReplicaCluster(IntegrationTestingUtility util, String tableName) {
    assertIsReplicaClusterUtil(util);
    // This createTable() call should throw an exception and take us to the catch block
    // fail() is supposed to not get executed
    try (Table ignored = createTable(util, tableName)) {
      fail(
        "An IOException should have occurred when trying to create a new table on the replica cluster");
    } catch (IOException e) {
      String expectedMsg = "Operation not allowed in Read-Only Mode";
      assertTrue("Expected an IOException with the following message: " + expectedMsg,
        e.getMessage().contains(expectedMsg));
      LOG.info("Successfully received an IOException when trying to create table '{}' on a "
        + "replica cluster", tableName);
    }
  }

  Table createTable(IntegrationTestingUtility util, String tableName) throws IOException {
    try (Table table = util.createTable(TableName.valueOf(tableName), COLUMN_FAMILY)) {
      return table;
    }
  }

  void createTableOnActiveCluster(IntegrationTestingUtility util, String tableName)
    throws IOException, InterruptedException {
    LOG.info("kevin: start createTableOnActiveCluster()");
    assertIsActiveClusterUtil(util);
    try (Table ignored = createTable(util, tableName)) {
      util.waitTableAvailable(TableName.valueOf(tableName));
    }
    LOG.info("kevin: end createTableOnActiveCluster()");
  }

  void refreshMeta(IntegrationTestingUtility util) throws IOException {
    LOG.info("kevin: start replicaAdmin.refreshMeta()");
    ProcedureExecutor<MasterProcedureEnv> procExecutor
      = util.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    long prodId = util.getAdmin().refreshMeta();
    util.waitForProcedureCompletion(prodId, procExecutor, 1000);
    LOG.info("kevin: end replicaAdmin.refreshMeta()");
  }

  void refreshHFiles(IntegrationTestingUtility util) throws IOException {
    LOG.info("kevin: start replicaAdmin.refreshHFiles()");
    ProcedureExecutor<MasterProcedureEnv> procExecutor
      = util.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    long prodId = util.getAdmin().refreshHFiles();
    util.waitForProcedureCompletion(prodId, procExecutor, 1000);
    LOG.info("kevin: end replicaAdmin.refreshHFiles()");
  }

  Result getRow(Connection conn, String tableName, String row) throws IOException {
    try (Table table = conn.getTable(TableName.valueOf(tableName))) {
      // Verify no data was added to the replica cluster
      Get get = new Get(Bytes.toBytes(row));
      get.addColumn(COLUMN_FAMILY, QUALIFIER);
      return table.get(get);
    }
  }

  void attemptPutOnReplicaCluster(IntegrationTestingUtility util, String tableName) {
    LOG.info("kevin: start attemptPutOnReplicaCluster()");
    assertIsReplicaClusterUtil(util);
    byte[] row = Bytes.toBytes("impossiblePutRow");
    try (Table replicaTable = util.getConnection().getTable(TableName.valueOf(tableName))) {
      // Add data to the active cluster
      Put put = new Put(row);
      put.addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes("impossibleValue"));
      replicaTable.put(put);
      fail("An IOException should have occurred when trying to perform a Put on the '"
        + tableName + "' table in the replica cluster");
    } catch (IOException e) {
      String expectedMsg = "Operation not allowed in Read-Only Mode";
      assertTrue("Expected an IOException with the following message: " + expectedMsg,
        e.getMessage().contains(expectedMsg));
    }
    LOG.info("kevin: end attemptPutOnReplicaCluster()");
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
