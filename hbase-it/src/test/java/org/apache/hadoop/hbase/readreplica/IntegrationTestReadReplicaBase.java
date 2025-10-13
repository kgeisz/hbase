package org.apache.hadoop.hbase.readreplica;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
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
import org.junit.BeforeClass;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public abstract class IntegrationTestReadReplicaBase extends IntegrationTestBase {
  public static final String IS_READ_REPLICA_INTEGRATION_TEST = null;
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestReadReplicaBase.class);
  protected static final String READ_ONLY_CONTROLLER_NAME = ReadOnlyController.class.getName();
  protected static final String CLUSTER_A_META_SUFFIX = "clusterA";
  protected static final String CLUSTER_B_META_SUFFIX = "clusterB";
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
  static MockedStatic<TableName> tableNameMockedStatic;

  @Override
  public void setUpCluster() throws Exception {
    LOG.info("kevin: start setUpCluster");

//    TableName clusterAMetaTable = TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR,
//      "meta_" + CLUSTER_A_META_SUFFIX);
//
//    tableNameMockedStatic = mockStatic(TableName.class);
//    tableNameMockedStatic.when(() -> TableName.initializeHbaseMetaTableName(any()))
//      .thenReturn(clusterAMetaTable);

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
    // confA.setInt("dfs.socket.timeout", 180*1000);
    confA.setInt("dfs.datanode.socket.write.timeout", 120*1000);
    confA.setInt("dfs.client.socket-timeout", 120*1000);
//    confA.setInt("test.hbase.zookeeper.property.clientPort", 2181);
    // confA.setInt("dfs.client-write-packet-timeout", 120*1000);

    LOG.info("kevin: starting clusterA minicluster");
    clusterA = utilA.startMiniCluster();
    String rootDir1 = clusterA.getConfiguration().get(HBASE_DIR);
    LOG.info("kevin: finished starting clusterA minicluster");
    connectionA = utilA.getConnection();

    // Use the active cluster's existing configuration to set up and start the replica cluster
//    TableName clusterBMetaTable = TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR,
//      "meta_" + CLUSTER_B_META_SUFFIX);
//    tableNameMockedStatic.when(() -> TableName.initializeHbaseMetaTableName(any()))
//      .thenReturn(clusterBMetaTable);
    confB = HBaseConfiguration.create(confA);
    confB.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    confB.set(HBASE_META_TABLE_SUFFIX, CLUSTER_B_META_SUFFIX);
//    confA.setInt("test.hbase.zookeeper.property.clientPort", 2182);
    utilB = new IntegrationTestingUtility(confB);
    utilB.setDataTestDirOnTestFS(utilA.getDataTestDirOnTestFS());
    utilB.setDFSCluster(utilA.getDFSCluster());
    System.setProperty("hbase.meta.table.suffix", CLUSTER_B_META_SUFFIX);
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

    RemoteIterator<LocatedFileStatus> f = fs.listFiles(new Path(clusterB.getConfiguration().get(HBASE_DIR)), false);

//    Thread.sleep(30*60*1000);

    LOG.info("kevin: confA dfs.datanode.handler.count = {}",
      confA.get("dfs.datanode.handler.count"));
    LOG.info("kevin: confA dfs.namenode.handler.count = {}",
      confA.get("dfs.namenode.handler.count"));
    LOG.info("kevin: confB dfs.datanode.handler.count = {}",
      confB.get("dfs.datanode.handler.count"));
    LOG.info("kevin: confB dfs.namenode.handler.count = {}",
      confB.get("dfs.namenode.handler.count"));

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

//    tableNameMockedStatic.close();

    LOG.info("kevin: end cleanUpCluster");
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
    LOG.info("kevin: start createTableOnActiveCluster()");
    LOG.info("kevin: creating table: {}", tableName);
    assertIsActiveClusterUtil(util);
    try (Table ignored = createTable(util, tableName)) {
      util.waitTableAvailable(TableName.valueOf(tableName));
    }
    LOG.info("kevin: end createTableOnActiveCluster()");
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

  void attemptFailedPutOnReplicaCluster(IntegrationTestingUtility util, String tableName) {
    LOG.info("kevin: start attemptFailedPutOnReplicaCluster()");
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
    LOG.info("kevin: end attemptFailedPutOnReplicaCluster()");
  }

  void putRowOnActiveCluster(IntegrationTestingUtility util, String tableName, String row) throws IOException {
    LOG.info("kevin: start putRowOnActiveCluster");
    assertIsActiveClusterUtil(util);
    try (Table activeTable = util.getConnection().getTable(TableName.valueOf(tableName))) {
      // Add data to the active cluster
      Put put = new Put(Bytes.toBytes(row));
      put.addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes("1"));
      LOG.info("about to perform put on active cluster");
      activeTable.put(put);
      LOG.info("done performing put on active cluster");
    }
    LOG.info("kevin: end putRowOnActiveCluster");
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
