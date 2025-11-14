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
import org.apache.hadoop.hbase.client.MutableRegionInfo;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.security.access.ReadOnlyController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Scanner;
import java.util.Set;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.HBASE_DIR;
import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_META_TABLE_SUFFIX;
import static org.apache.hadoop.hbase.util.CommonFSUtils.HBASE_WAL_DIR;
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
  protected static Field metaTableName;
  protected static Field firstMetaRegionInfo;
  protected static Field masterRegionDirName;
  protected static Object originalMetaTableName;
  protected static Object originalFirstMetaRegionInfo;
  protected Object originalFirstMetaRegionInfoForClusterA;
  protected Object originalFirstMetaRegionInfoForClusterB;
  protected Object originalMasterRegionDirName;
  SingleProcessHBaseCluster clusterA;
  SingleProcessHBaseCluster clusterB;
  MiniDFSCluster dfsCluster;
  protected IntegrationTestingUtility utilA;
  protected IntegrationTestingUtility utilB;
  protected Configuration confA;
  protected Configuration confB;
  Connection connectionA;
  Connection connectionB;
  Path rootDir;
  FileSystem fs;

  MiniZooKeeperCluster zkA;

  // TODO - move this method to somewhere more public since it's used in multiple classes now
  // (such as HBaseTestingUtil)
  /**
   * A helper method to modify a static final field using reflection. This is necessary for testing
   * code that reads a configuration only once during class loading.
   * @param field    The field to modify.
   * @param newValue The new value to set.
   * @throws Exception if reflection fails.
   */
  private static void setStaticFinalField(Field field, Object newValue) throws Exception {
    field.setAccessible(true);
    // Using MethodHandles to get a trusted lookup with the necessary permissions to modify it.
    // NOTE: For this to work, the JVM running the test must be started with arguments like:
    // --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
    var lookup = MethodHandles.privateLookupIn(Field.class, MethodHandles.lookup());
    var handle = lookup.findVarHandle(Field.class, "modifiers", int.class);
    handle.set(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(null, newValue);
  }

  // Re-initialize the static final META_TABLE_NAME variable for the testing of a non-default value
  protected void reinitializeStaticMetaTableName(Configuration conf) throws Exception {
    TableName expectedMetaTableName = TableName.initializeHbaseMetaTableName(conf);
    setStaticFinalField(metaTableName, expectedMetaTableName);
    LOG.info("kevin: reinitialized META_TABLE_NAME to: {}", expectedMetaTableName);
  }

  // Re-initialize the static final FIRST_META_REGIONINFO variable for starting a new cluster after
  // META_TABLE_NAME has changed. This method should be used after reinitializeStaticMetaTableName()
//  private void reinitializeFirstMetaRegionInfo() throws Exception {
//    RegionInfo expectedFirstMetaRegionInfo = new MutableRegionInfo(
//      1L, TableName.META_TABLE_NAME, RegionInfo.DEFAULT_REPLICA_ID);
//    setStaticFinalField(firstMetaRegionInfo, expectedFirstMetaRegionInfo);
//    LOG.info("kevin: reinitialized FIRST_META_REGIONINFO to: {}", expectedFirstMetaRegionInfo);
//  }

  protected void reinitializeFirstMetaRegionInfoForClusterA() throws Exception {
    if (originalFirstMetaRegionInfoForClusterA == null) {
      originalFirstMetaRegionInfoForClusterA = new MutableRegionInfo(
        1L, TableName.META_TABLE_NAME, RegionInfo.DEFAULT_REPLICA_ID);
    }
    if (firstMetaRegionInfo == null) {
      firstMetaRegionInfo = RegionInfoBuilder.class.getDeclaredField("FIRST_META_REGIONINFO");
    }
    setStaticFinalField(firstMetaRegionInfo, originalFirstMetaRegionInfoForClusterA);
    LOG.info("kevin: reinitialized FIRST_META_REGIONINFO for Cluter A to: {}", originalFirstMetaRegionInfoForClusterA);
  }

  protected void reinitializeFirstMetaRegionInfoForClusterB() throws Exception {
    if (originalFirstMetaRegionInfoForClusterB == null) {
      originalFirstMetaRegionInfoForClusterB = new MutableRegionInfo(
        1L, TableName.META_TABLE_NAME, RegionInfo.DEFAULT_REPLICA_ID);
    }
    if (firstMetaRegionInfo == null) {
      firstMetaRegionInfo = RegionInfoBuilder.class.getDeclaredField("FIRST_META_REGIONINFO");
    }
    setStaticFinalField(firstMetaRegionInfo, originalFirstMetaRegionInfoForClusterB);
    LOG.info("kevin: reinitialized FIRST_META_REGIONINFO for Cluster B to: {}", originalFirstMetaRegionInfoForClusterB);
  }

  protected void reinitializeMasterRegionDirName(Configuration conf) throws Exception {
    String expectedMasterRegionDirName = MasterRegionFactory.initMasterRegionDirName(conf);
    setStaticFinalField(masterRegionDirName, expectedMasterRegionDirName);
    LOG.info("kevin: reinitialized MASTER_REGION_DIR_NAME to: {}", expectedMasterRegionDirName);
  }

//  protected void reinitializeStaticVariables(Configuration conf) throws Exception {
//    reinitializeStaticMetaTableName(conf);
//    reinitializeFirstMetaRegionInfo();
//  }

  protected void runInputScanner() {
    Scanner scanner = new Scanner(System.in);
    System.out.println("kevin: Waiting for the user to input a line");
    String line = scanner.nextLine();
    System.out.printf("kevin: Got a line: '%s'. Continuing%n", line);
  }

  protected void scanMetaTable(Connection connection) throws IOException {
    Scan scan = new Scan();
    Table metaTable = connection.getTable(TableName.META_TABLE_NAME);
    ResultScanner scanner = metaTable.getScanner(scan);
    for (Result scannerResult : scanner) {
      LOG.info("kevin: scanner result = {}", scannerResult);
    }
  }

  @Override
  public void setUpCluster() throws Exception {
    // Save the original value of META_TABLE_NAME and FIRST_META_REGIONINFO before any test runs
    metaTableName = TableName.class.getDeclaredField("META_TABLE_NAME");
//    originalMetaTableName = metaTableName.get(null);
    firstMetaRegionInfo = RegionInfoBuilder.class.getDeclaredField("FIRST_META_REGIONINFO");
//    originalFirstMetaRegionInfo = firstMetaRegionInfo.get(null);
    masterRegionDirName = MasterRegionFactory.class.getDeclaredField("MASTER_REGION_DIR_NAME");
//    originalMasterRegionDirName = masterRegionDirName.get(null);

    // Set up and start the active cluster
    util = new IntegrationTestingUtility(); // The test fails if util is not set
    utilA = util;
    confA = utilA.getConfiguration();
    confA.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    confA.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    confA.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    confA.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, READ_ONLY_CONTROLLER_NAME);
    confA.set(HBASE_META_TABLE_SUFFIX, CLUSTER_A_META_SUFFIX);
    Path walDirClusterA = new Path("test-data", "wal_" + CLUSTER_A_META_SUFFIX);
    confA.set(HBASE_WAL_DIR, String.valueOf(walDirClusterA));
    Path thing = util.getDataTestDirOnTestFS();
    // Minimize resource contention within the DFS
    confA.setInt("dfs.datanode.handler.count", 1);
    confA.setInt("dfs.namenode.handler.count", 1);
    // Prevent retries for Puts on the replica cluster that are expected to fail
    confA.setInt(HBASE_CLIENT_RETRIES_NUMBER, 0);
    confA.setInt("dfs.datanode.socket.write.timeout", 120*1000);
    confA.setInt("dfs.client.socket-timeout", 120*1000);
    confA.setInt("hbase.master.start.timeout.localHBaseCluster", 30*60*1000);

//    reinitializeStaticVariables(confA);
    reinitializeStaticMetaTableName(confA);
    reinitializeFirstMetaRegionInfoForClusterA();
    reinitializeMasterRegionDirName(confA);

    LOG.info("kevin: starting Cluster A minicluster as the active cluster");
    clusterA = utilA.startMiniCluster();
//    String rootDir1 = clusterA.getConfiguration().get(HBASE_DIR);
    connectionA = utilA.getConnection();

    fs = clusterA.getMaster().getFileSystem();
    rootDir = new Path(confA.get(HBASE_DIR));
    dfsCluster = utilA.getDFSCluster();
    assertProperActiveClusterInitialization(utilA, CLUSTER_A_META_SUFFIX);

    LOG.info("kevin: Cluster A HBASE_WAL_DIR = {}", confA.get(HBASE_WAL_DIR));
    LOG.info("kevin: Cluster A HBASE_DIR = {}", confA.get(HBASE_DIR));

//    runInputScanner();

    // Use the active cluster's existing configuration to set up the replica cluster, but don't
    // start it.
    confB = HBaseConfiguration.create(confA);
    confB.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    confB.set(HBASE_META_TABLE_SUFFIX, CLUSTER_B_META_SUFFIX);
    Path walDirClusterB = new Path("test-data", "wal_" + CLUSTER_B_META_SUFFIX);
    confB.set(HBASE_WAL_DIR, String.valueOf(walDirClusterB));
    confB.setInt("hbase.master.start.timeout.localHBaseCluster", 30*60*1000);
    utilB = new IntegrationTestingUtility(confB);
    utilB.setDataTestDirOnTestFS(utilA.getDataTestDirOnTestFS());
    utilB.setDFSCluster(dfsCluster);
//    utilB.setClusterTestDir(utilA.getClusterTestDir());
  }

  protected Thread[] getGroupThreads( final ThreadGroup group ) {
    if ( group == null )
      throw new NullPointerException( "Null thread group" );
    int nAlloc = group.activeCount( );
    int n = 0;
    Thread[] threads;
    do {
      nAlloc *= 2;
      threads = new Thread[ nAlloc ];
      n = group.enumerate( threads );
    } while ( n == nAlloc );
    return java.util.Arrays.copyOf( threads, n );
  }

  @Override
  public void cleanUpCluster() throws Exception {
    LOG.info("kevin: start cleanUpCluster()");
    if (connectionA != null && !connectionA.isClosed()) {
      connectionA.close();
    }

    if (connectionB != null && !connectionB.isClosed()) {
      connectionB.close();
    }

    // Shutdown Cluster B's resources
    if (utilB != null) {
      if (utilB.isMiniClusterRunning()) {
        LOG.info("Shutting down Cluster B's mini HBase cluster");
        utilB.shutdownMiniHBaseCluster();
      }

      if (utilB.getZkCluster() != null) {
        LOG.info("Shutting down Cluster B's mini Zookeeper cluster");
        utilB.shutdownMiniZKCluster();
      }
    }

    // Shutdown Cluster A's resources
    if (utilA != null) {
      if (utilA.isMiniClusterRunning()) {
        LOG.info("Shutting down Cluster A's mini HBase cluster");
        utilA.shutdownMiniHBaseCluster();
      }

      if (utilA.getDFSCluster() != null) {
        LOG.info("Shutting down mini DFS cluster");
        utilA.shutdownMiniDFSCluster();
      }

      if (utilA.getZkCluster() != null) {
        LOG.info("Shutting down Cluster A's mini Zookeeper cluster");
        utilA.shutdownMiniZKCluster();
      }
    }

//    LOG.info("Restoring Cluster A");
//    utilA.restoreCluster();
    LOG.info("kevin: end cleanUpCluster()");
  }

  protected void assertProperActiveClusterInitialization(IntegrationTestingUtility util, String suffix) throws IOException {
    Configuration conf = util.getConfiguration();

    assertFalse(
      "The active cluster should have " + HBASE_GLOBAL_READONLY_ENABLED_KEY + " set to false",
      Boolean.parseBoolean(conf.get(HBASE_GLOBAL_READONLY_ENABLED_KEY)));

    assertProperMasterDataDirectory(suffix);

    assertTrue("The active cluster should have the following meta table name: hbase:meta_" + suffix,
      isMetaTableNameCorrect(suffix));
    assertHBaseMetaDirExists(suffix);

    assertEquals("The original DFS cluster is expected to be used", dfsCluster, util.getDFSCluster());

    assertProperDFSClusterIsUsed(util);
  }

  protected void assertProperReplicaClusterInitialization(IntegrationTestingUtility util, String suffix) throws IOException {
    Configuration conf = util.getConfiguration();

    assertEquals("The replica cluster should be using the same root directory as the active cluster",
      conf.get(HBASE_DIR), rootDir.toString());

    assertTrue(
      "The replica cluster should have " + HBASE_GLOBAL_READONLY_ENABLED_KEY + " set to true",
      Boolean.parseBoolean(conf.get(HBASE_GLOBAL_READONLY_ENABLED_KEY)));

    assertProperMasterDataDirectory(suffix);

    assertTrue("The replica cluster should have the following meta table name: hbase:meta_" + suffix,
      isMetaTableNameCorrect(suffix));
    assertHBaseMetaDirExists(suffix);

    assertProperDFSClusterIsUsed(util);
  }

  private void assertProperDFSClusterIsUsed(IntegrationTestingUtility util) {
    assertEquals("Expected this IntegrationTestingUtility to be using the original DFS "
      + "cluster that was created for the active cluster during test initialization",
      dfsCluster, util.getDFSCluster());
  }

  private boolean isMetaTableNameCorrect(String suffix) {
    return (TableName.META_TABLE_NAME.getNameAsString().equals("hbase:meta_" + suffix));
  }

  private void assertHBaseMetaDirExists(String suffix) throws IOException {
    String metaDir = "data" + File.separator + "hbase" + File.separator + "meta_" + suffix;
    Path metaDirPath = new Path(rootDir, metaDir);
    assertTrue("Expected HBase meta directory to exist at: " + metaDirPath, fs.exists(metaDirPath));
  }

  private void assertProperMasterDataDirectory(String suffix) throws IOException {
    String clusterMasterDataDir = "MasterData_" + suffix;
    assertTrue("Expected " + clusterMasterDataDir + " to exist in the filesystem",
      fs.exists(new Path(rootDir, clusterMasterDataDir)));
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
