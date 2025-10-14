package org.apache.hadoop.hbase.readreplica;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.mockito.MockedStatic;
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
  protected static final String CLUSTER_A_META_SUFFIX = "clusterA";
  protected static final String CLUSTER_B_META_SUFFIX = "clusterB";
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
    // Set up and start the active cluster
    util = new IntegrationTestingUtility(); // The test fails if util is not set
    utilA = util;
    confA = utilA.getConfiguration();
    confA.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    confA.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    confA.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    confA.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    confA.set(HBASE_META_TABLE_SUFFIX, CLUSTER_A_META_SUFFIX);
    // Minimize resource contention within the DFS
    confA.setInt("dfs.datanode.handler.count", 1);
    confA.setInt("dfs.namenode.handler.count", 1);
    // Prevent retries for Puts on the replica cluster that are expected to fail
    confA.setInt(HBASE_CLIENT_RETRIES_NUMBER, 0);
    confA.setInt("dfs.datanode.socket.write.timeout", 120*1000);
    confA.setInt("dfs.client.socket-timeout", 120*1000);

    LOG.info("Starting Cluster A minicluster as the active cluster");
    clusterA = utilA.startMiniCluster();
    String rootDir1 = clusterA.getConfiguration().get(HBASE_DIR);
    connectionA = utilA.getConnection();

    // Use the active cluster's existing configuration to set up and start the replica cluster
    confB = HBaseConfiguration.create(confA);
    confB.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    confB.set(HBASE_META_TABLE_SUFFIX, CLUSTER_B_META_SUFFIX);
    utilB = new IntegrationTestingUtility(confB);
    utilB.setDataTestDirOnTestFS(utilA.getDataTestDirOnTestFS());
    utilB.setDFSCluster(utilA.getDFSCluster());
    System.setProperty("hbase.meta.table.suffix", CLUSTER_B_META_SUFFIX);
    LOG.info("Starting Cluster B minicluster as the replica cluster on the same DFS as Cluster A");
    clusterB = utilB.startMiniCluster();
    String rootDir2 = clusterB.getConfiguration().get(HBASE_DIR);
    connectionB = utilB.getConnection();

    fs = clusterA.getMaster().getFileSystem();
    assertProperInitialization(rootDir1, rootDir2);
  }

  @Override
  public void cleanUpCluster() throws Exception {
    connectionA.close();
    connectionB.close();

    LOG.info("Shutting down Cluster B's mini HBase cluster");
    utilB.shutdownMiniHBaseCluster();

    LOG.info("Shutting down Cluster B's mini Zookeeper cluster");
    utilB.shutdownMiniZKCluster();

    LOG.info("Not shutting down Cluster B's DFS since it is shared with Cluster A");

    LOG.info("Restoring Cluster A");
    utilA.restoreCluster();
  }

  private void assertProperInitialization(String rootDir1, String rootDir2) throws IOException, InterruptedException {
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
    String clusterAMasterDataDir = MasterRegionFactory.MASTER_STORE_DIR + "_" + CLUSTER_A_META_SUFFIX;
    assertTrue("Expected " + clusterAMasterDataDir + " to exist in the filesystem",
      fs.exists(new Path(rootDir, clusterAMasterDataDir)));
    String clusterBMasterDataDir = MasterRegionFactory.MASTER_STORE_DIR + "_" + CLUSTER_B_META_SUFFIX;
    assertTrue("Expected " + clusterBMasterDataDir + " to exist in the filesystem",
      fs.exists(new Path(rootDir, clusterBMasterDataDir)));
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

  void attemptFailedCreateOnReplicaCluster(IntegrationTestingUtility util, String tableName) {
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
    assertIsActiveClusterUtil(util);
    try (Table ignored = createTable(util, tableName)) {
      util.waitTableAvailable(TableName.valueOf(tableName));
    }
  }

  boolean doesClusterHaveTable(IntegrationTestingUtility util, String tableName)
    throws IOException {
    return Arrays.stream(util.getAdmin().listTableNames()).toList().contains(TableName.valueOf(tableName));
  }

  void assertTableExistsOnActiveCluster(IntegrationTestingUtility util, String tableName) throws IOException {
    assertIsActiveClusterUtil(util);
    assertTrue("The active cluster should have a table called " + tableName,
      doesClusterHaveTable(util, tableName));
  }

  void assertTableExistsOnReplicaCluster(IntegrationTestingUtility util, String tableName) throws IOException {
    assertIsReplicaClusterUtil(util);
    assertTrue("The replica cluster should have a table called " + tableName,
      doesClusterHaveTable(util, tableName));
  }
  void assertTableDoesNotExistOnReplicaCluster(IntegrationTestingUtility util, String tableName)
    throws IOException {
    assertIsReplicaClusterUtil(util);
    assertFalse("The replica cluster should not have a table called " + tableName,
      doesClusterHaveTable(util, tableName));
  }

  void refreshMeta(IntegrationTestingUtility util) throws IOException {
    ProcedureExecutor<MasterProcedureEnv> procExecutor
      = util.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    long prodId = util.getAdmin().refreshMeta();
    util.waitForProcedureCompletion(prodId, procExecutor, 1000);
  }

  void refreshHFiles(IntegrationTestingUtility util) throws IOException {
    ProcedureExecutor<MasterProcedureEnv> procExecutor
      = util.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    long prodId = util.getAdmin().refreshHFiles();
    util.waitForProcedureCompletion(prodId, procExecutor, 1000);
  }

  Result getRow(Connection conn, String tableName, String row) throws IOException {
    try (Table table = conn.getTable(TableName.valueOf(tableName))) {
      // Verify no data was added to the replica cluster
      Get get = new Get(Bytes.toBytes(row));
      get.addColumn(COLUMN_FAMILY, QUALIFIER);
      return table.get(get);
    }
  }

  void attemptFailedPutOnReplicaCluster(IntegrationTestingUtility util, String tableName) {
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
  }

  void putRowOnActiveCluster(IntegrationTestingUtility util, String tableName, String row) throws IOException {
    assertIsActiveClusterUtil(util);
    try (Table activeTable = util.getConnection().getTable(TableName.valueOf(tableName))) {
      // Add data to the active cluster
      Put put = new Put(Bytes.toBytes(row));
      put.addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes("1"));
      activeTable.put(put);
    }
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
