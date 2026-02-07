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
package org.apache.hadoop.hbase.backup;

import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_BACKUP_MAX_WAL_SIZE;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_STAGED_WAL_FLUSH_INITIAL_DELAY;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.CONF_STAGED_WAL_FLUSH_INTERVAL;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.apache.hadoop.hbase.mapreduce.WALPlayer.IGNORE_EMPTY_FILES;
import static org.apache.hadoop.hbase.mapreduce.WALPlayer.IGNORE_MISSING_FILES;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

public class IntegrationTestPointInTimeRestore extends IntegrationTestBackupRestoreBase {
  private static final String CLASS_NAME = IntegrationTestPointInTimeRestore.class.getSimpleName();
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestPointInTimeRestore.class);

  private static Path restoreRootDir;

  @Override
  @Before
  public void setUp() throws Exception {
    initializeTestParameters();
    BackupTestUtil.enableBackup(conf);
    conf.set(CONF_BACKUP_MAX_WAL_SIZE, "10240");
    conf.set(CONF_STAGED_WAL_FLUSH_INITIAL_DELAY, "10");
    conf.set(CONF_STAGED_WAL_FLUSH_INTERVAL, "10");
    conf.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);
    conf.setBoolean(IGNORE_EMPTY_FILES, true);
    conf.setBoolean(IGNORE_MISSING_FILES, true);

    LOG.info("Initializing cluster with {} region server(s)", regionServerCount);
    util.initializeCluster(regionServerCount);
    LOG.info("Cluster initialized and ready");

    backupRootDir = util.getDataTestDirOnTestFS() + Path.SEPARATOR + DEFAULT_BACKUP_ROOT_DIR;
    LOG.info("The backup root directory is: {}", backupRootDir);
    createAndSetBackupWalDir();
    fs = FileSystem.get(conf);

    restoreRootDir = BackupUtils.getTmpRestoreOutputDir(FileSystem.get(conf), conf);
    LOG.info("kevin: restoreRootDir is: {}", restoreRootDir);

    rowsInIteration = 100;
  }

  @Test
  public void testPointInTimeRestore() throws Exception {
    LOG.info("Running Point-In-Time-Restore integration test");
    TableName tableName = TableName.valueOf(CLASS_NAME + ".continuous");
    try (Connection conn = util.getConnection(); BackupAdmin client = new BackupAdminImpl(conn);
      Table tableConn = conn.getTable(tableName)) {

      // runPitrFailureFromUsingNonContinuousFullBackup(client);
      runFailedPartialRestore(client);

//      setEnvironmentEdgeToNumDaysAgo(30);
//
//      createTable(tableName);
//      List<TableName> tables = Lists.newArrayList(tableName);
//      List<String> backupIds = new ArrayList<>();
//
//      // Add rows to the table
//      loadData(tableName, rowsInIteration);
//      int preFullBackupRowCount = PITRTestUtil.getRowCount(util, tableName);
//      LOG.info("kevin: Current row count for table {} before continuous full backup is: {}",
//        tableName, preFullBackupRowCount);
//
//      createContinuousFullBackup(client, tableName, tables);
//
//      setEnvironmentEdgeToNumDaysAgo(25);
//
//      // Add rows to the table
//      LOG.info("kevin: Loading more data into table {} after performing PITR", tableName);
//      loadData(tableName, rowsInIteration);
//      int postFullBackupRowCount = PITRTestUtil.getRowCount(util, tableName);
//      LOG.info("kevin: Current row count for table after full backup {} is: {}", tableName,
//        postFullBackupRowCount);
//
//      int numDaysAgo = 20;
//      setEnvironmentEdgeToNumDaysAgo(numDaysAgo);
//
////      runInputScanner();
//
//      // runPitrValidationOnlyModeTestCase(client, tableName);
//      // runPitrFailureFromUsingTimeBeforeOldestBackup(client, tableName);
//      // runPitrFailureFromUsingDateAfterRetentionWindow(client, tableName);
//      runSuccessfulPitr(client, tableName, preFullBackupRowCount, 26);
//
//      // The original table still has all of its rows
//      int expectedRowCount = 200;
//      assertEquals("The original table should still have " + expectedRowCount + " rows",
//        expectedRowCount, PITRTestUtil.getRowCount(util, tableName));
//
//      for (int i = 1; i <= 5; i++) {
//        numDaysAgo = numDaysAgo - 2;
//        setEnvironmentEdgeToNumDaysAgo(numDaysAgo);
//
//        loadData(tableName, rowsInIteration);
//        int preIncrementalBackupRowCount = PITRTestUtil.getRowCount(util, tableName);
//
//        long latestPutTimestamp = getLatestPutTimestamp(tableConn);
//        waitForCheckpointTimestampsToUpdate(conn, latestPutTimestamp, tableName);
//
//        LOG.info("kevin: Creating incremental backup number {} for table {}", i, tableName);
//        BackupRequest.Builder builder = new BackupRequest.Builder();
//        BackupRequest request = builder.withBackupType(BackupType.INCREMENTAL).withTableList(tables)
//          .withTargetRootDir(backupRootDir).withContinuousBackupEnabled(true)
//          .build();
//        String incrementalBackupId = backup(request, client, backupIds);
//        LOG.info("kevin: Created incremental backup number {} with ID: {}", i, incrementalBackupId);
//
//        loadData(tableName, rowsInIteration);
//
//        runSuccessfulPitr(client, tableName, preIncrementalBackupRowCount, numDaysAgo + 1);
//
//        // The original table still has all of its rows
//        expectedRowCount = expectedRowCount + 2 * rowsInIteration;
//        assertEquals("The original table should still have " + expectedRowCount + " rows",
//          expectedRowCount, PITRTestUtil.getRowCount(util, tableName));
//      }
//
////       runInputScanner();
    }
  }

  private void runPitrFailureFromUsingNonContinuousFullBackup(BackupAdmin client) throws Exception {
    // Create a table that will be backed up via a non-continuous full backup
    TableName tableName = TableName.valueOf(CLASS_NAME + ".non-continuous");
    createTable(tableName);
    TableName restoredTable = TableName.valueOf("restoredTable");

    setEnvironmentEdgeToNumDaysAgo(20);

    // Add rows to the table
    loadData(tableName, rowsInIteration);

    String fullBackupId = createNonContinuousFullBackup(client, tableName);

    setEnvironmentEdgeToNumDaysAgo(10);

    // Add rows to the table
    LOG.info("kevin: Loading more data into table {} after performing PITR", tableName);
    loadData(tableName, rowsInIteration);

    // Create a PITR request on the table
    PointInTimeRestoreRequest.Builder pitrBuilder = new PointInTimeRestoreRequest.Builder();
    PointInTimeRestoreRequest pitrRequest = pitrBuilder.withBackupRootDir(backupRootDir)
      .withFromTables(new TableName[] { tableName }).withToTables(new TableName[] { restoredTable })
      .withToDateTime(System.currentTimeMillis() - 11 * ONE_DAY_IN_MILLISECONDS)
      .withRestoreRootDir(restoreRootDir.toString()).withCheck(false).build();

    // Run a PITR that should fail since the full backup is not continuous
    LOG.info("kevin: Running Point-In-Time-Restore on a non-continuous full backup");
    try {
      pointInTimeRestore(pitrRequest, client);
      fail(
        "An IOException should have occurred due to running PITR on a non-continuous full backup");
    } catch (IOException e) {
      assertTrue(e.getMessage()
        .contains("Continuous Backup is not enabled for the following " + "tables: " + tableName));
      LOG.info("kevin: Got expected IOException after PITR on a non-continuous full backup");
    }

    // This table is no longer needed
    LOG.info("kevin: Deleting non-continuous full backup with ID: {}", fullBackupId);
    delete(new String[] { fullBackupId }, client);
  }

  // The PITR fails due to one table not satisfying the PITR contitions
  private void runFailedPartialRestore(BackupAdmin client) throws Exception {
    List<TableName> tableNames = new ArrayList<>();
    List<TableName> restoredTableNames = new ArrayList<>();
    List<String> fullBackupIds = new ArrayList<>();

    setEnvironmentEdgeToNumDaysAgo(20);

    // Create a table that will be backed up via a non-continuous full backup
    TableName nonContinuousTableName = TableName.valueOf(CLASS_NAME + ".non-continuous");
    createTable(nonContinuousTableName);
    tableNames.add(nonContinuousTableName);
    restoredTableNames.add(TableName.valueOf("nonContinuousRestoredTable"));

    // Create a table that will be backed up via a continuous full backup
    TableName continuousTableName = TableName.valueOf(CLASS_NAME + ".continuous");
    createTable(continuousTableName);
    tableNames.add(continuousTableName);
    restoredTableNames.add(TableName.valueOf("continuousRestoredTable"));

    loadDataIntoTables(tableNames);

    // Run non-continuous and continuous full backups
    String fullBackupId = createNonContinuousFullBackup(client, nonContinuousTableName);
    fullBackupIds.add(fullBackupId);
    fullBackupId = createContinuousFullBackup(client, List.of(continuousTableName));
    fullBackupIds.add(fullBackupId);


    setEnvironmentEdgeToNumDaysAgo(10);

    // Add rows to the table
    loadDataIntoTables(tableNames);

    // Create a PITR request on the tables
    PointInTimeRestoreRequest.Builder pitrBuilder = new PointInTimeRestoreRequest.Builder();
    PointInTimeRestoreRequest pitrRequest = pitrBuilder.withBackupRootDir(backupRootDir)
      .withFromTables(tableNames.toArray(new TableName[0]))
      .withToTables(restoredTableNames.toArray(new TableName[0]))
      .withToDateTime(System.currentTimeMillis() - 11 * ONE_DAY_IN_MILLISECONDS)
      .withRestoreRootDir(restoreRootDir.toString()).withCheck(false).build();

    // Run a PITR that should fail since the full backup is not continuous
    LOG.info("kevin: Running Point-In-Time-Restore on a non-continuous full backup");
    try {
      pointInTimeRestore(pitrRequest, client);
      fail(
        "An IOException should have occurred due to running PITR on a non-continuous full backup");
    } catch (IOException e) {
      assertTrue(e.getMessage()
        .contains("Continuous Backup is not enabled for the following tables: " + nonContinuousTableName));
      LOG.info("kevin: Got expected IOException after PITR on a non-continuous full backup");
    }

    // These tables are no longer needed
    LOG.info("kevin: Deleting full backups with IDs: {}", fullBackupIds);
    delete(fullBackupIds.toArray(new String[0]), client);
  }

  private String createNonContinuousFullBackup(BackupAdmin client, TableName tableName)
    throws IOException {
    LOG.info("kevin: Intentionally creating a full backup for table {} with continuous backup "
      + "DISABLED", tableName);
    List<TableName> tables = Lists.newArrayList(tableName);
    BackupRequest.Builder fullBackupBuilder = new BackupRequest.Builder();
    BackupRequest fullBackupRequest =
      fullBackupBuilder.withBackupType(BackupType.FULL).withTableList(tables)
        .withTargetRootDir(backupRootDir).withContinuousBackupEnabled(false).build();
    String fullBackupId = backup(fullBackupRequest, client, null);
    LOG.info("Created full non-continuous backup with ID: {}", fullBackupId);
    return fullBackupId;
  }

  private String createContinuousFullBackup(BackupAdmin client, List<TableName> tables) throws IOException {
    // Create a full backup for the table
    LOG.info("Creating full backup image with continuous backup enabled for the followint table(s)", tables);
    BackupRequest.Builder fullBackupBuilder = new BackupRequest.Builder();
    BackupRequest fullBackupRequest =
      fullBackupBuilder.withBackupType(BackupType.FULL).withTableList(tables)
        .withTargetRootDir(backupRootDir).withContinuousBackupEnabled(true).build();
    String fullBackupId = backup(fullBackupRequest, client, null);
    LOG.info("Created full backup with ID: {}", fullBackupId);
    return fullBackupId;
  }

  private void runPitrValidationOnlyModeTestCase(BackupAdmin client, TableName tableName)
    throws IOException {
    TableName restoredTable = TableName.valueOf("restoredTable");

    // Create request for PITR in validation-only mode
    PointInTimeRestoreRequest.Builder pitrBuilder = new PointInTimeRestoreRequest.Builder();
    PointInTimeRestoreRequest pitrRequest = pitrBuilder.withBackupRootDir(backupRootDir)
      .withFromTables(new TableName[] { tableName }).withToTables(new TableName[] { restoredTable })
      .withToDateTime(System.currentTimeMillis() - 11 * ONE_DAY_IN_MILLISECONDS)
      .withRestoreRootDir(restoreRootDir.toString()).withCheck(true).build();

    // Run PITR in validation-only mode
    LOG.info("kevin: Running Point-In-Time-Restore in validation-only mode");
    pointInTimeRestore(pitrRequest, client);
    LOG.info("kevin: Finished running Point-In-Time-Restore in validation-only mode");
  }

  private void runPitrFailureFromUsingTimeBeforeOldestBackup(BackupAdmin client,
    TableName tableName) throws IOException {
    TableName restoredTable = TableName.valueOf("restoredTable");

    // Create bad PITR request due to date being before the oldest backup
    PointInTimeRestoreRequest.Builder pitrBuilder = new PointInTimeRestoreRequest.Builder();
    PointInTimeRestoreRequest pitrRequest = pitrBuilder.withBackupRootDir(backupRootDir)
      .withFromTables(new TableName[] { tableName }).withToTables(new TableName[] { restoredTable })
      .withToDateTime(System.currentTimeMillis() - 21 * ONE_DAY_IN_MILLISECONDS)
      .withRestoreRootDir(restoreRootDir.toString()).withCheck(false).build();

    // Run actual PITR
    LOG.info("kevin: Performing faulty Point-In-Time-Restore on table {} due to bad restore date",
      tableName);
    try {
      pointInTimeRestore(pitrRequest, client);
      fail("Expected PITR to fail due to bad restore date");
    } catch (IOException e) {
      assertTrue(e.getMessage()
        .contains("PITR failed: No valid backup/WALs found for source " + "table " + tableName));
      LOG.info(
        "kevin: Got expected IOException after attempting PITR using a date before the oldest backup");
    }
  }

  private void runPitrFailureFromUsingDateAfterRetentionWindow(BackupAdmin client,
    TableName tableName) {
    TableName restoredTable = TableName.valueOf("restoredTable");

    long requestedRecoveryTime = System.currentTimeMillis() + ONE_DAY_IN_MILLISECONDS;

    // Create bad PITR request due to date being before the oldest backup
    PointInTimeRestoreRequest.Builder pitrBuilder = new PointInTimeRestoreRequest.Builder();
    PointInTimeRestoreRequest pitrRequest =
      pitrBuilder.withBackupRootDir(backupRootDir).withFromTables(new TableName[] { tableName })
        .withToTables(new TableName[] { restoredTable }).withToDateTime(requestedRecoveryTime)
        .withRestoreRootDir(restoreRootDir.toString()).withCheck(false).build();

    // Run actual PITR
    LOG.info("kevin: Performing faulty Point-In-Time-Restore on table {} due to restore date "
      + "after retention window", tableName);
    try {
      pointInTimeRestore(pitrRequest, client);
      fail("Expected PITR to fail due to using date after retention window");
    } catch (IOException e) {
      assertTrue(e.getMessage()
        .contains("Requested recovery time (" + requestedRecoveryTime + ") " + "is in the future"));
      LOG.info("kevin: Got expected IOException after attempting PITR using a date after the "
        + "retention window");
    }
  }

  private void runSuccessfulPitr(BackupAdmin client, TableName tableName,
    int expectedPostPitrRowCount, int toDaysAgo) throws IOException {
    TableName restoredTable = TableName.valueOf("restoredTable");

    // Create actual PITR request
    PointInTimeRestoreRequest.Builder pitrBuilder = new PointInTimeRestoreRequest.Builder();
    PointInTimeRestoreRequest pitrRequest = pitrBuilder.withBackupRootDir(backupRootDir)
      .withFromTables(new TableName[] { tableName }).withToTables(new TableName[] { restoredTable })
      .withToDateTime(System.currentTimeMillis() - toDaysAgo * ONE_DAY_IN_MILLISECONDS)
      .withRestoreRootDir(restoreRootDir.toString()).withCheck(false).withOverwrite(true).build();

    // Run actual PITR
    LOG.info("kevin: Performing Point-In-Time-Restore on table {} and restoring to table {}",
      tableName, restoredTable);
    pointInTimeRestore(pitrRequest, client);
    LOG.info("kevin: Finished Point-In-Time-Restore on table {} to table {}", tableName,
      restoredTable);

    // Verify row count post-PITR
    int postPitrRowCount1 = PITRTestUtil.getRowCount(util, restoredTable);
    LOG.info("kevin: Current row count for table {} after PITR is: {}", tableName,
      postPitrRowCount1);
    assertEquals(expectedPostPitrRowCount, expectedPostPitrRowCount,
      postPitrRowCount1);
  }

  private void setEnvironmentEdgeToNumDaysAgo(int numDaysAgo) {
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - numDaysAgo * ONE_DAY_IN_MILLISECONDS);
    LOG.info("kevin: Finished setting to {} days ago", numDaysAgo);
    LOG.info("kevin: EnvironmentEdgeManager current time: {}",
      EnvironmentEdgeManager.currentTime());
  }

  protected void runInputScanner() {
    Scanner scanner = new Scanner(System.in);
    System.out.println("kevin: Waiting for the user to input a line");
    String line = scanner.nextLine();
    System.out.printf("kevin: Got a line: '%s'. Continuing%n", line);
  }

  private void loadDataIntoTables(List<TableName> tableNames) throws IOException {
    for (TableName tableName : tableNames) {
      LOG.info("kevin: Loading data into table {}", tableName);
      loadData(tableName, rowsInIteration);
    }
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    return 0;
  }
}
