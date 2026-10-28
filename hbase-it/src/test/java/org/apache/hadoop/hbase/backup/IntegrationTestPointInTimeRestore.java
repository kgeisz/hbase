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

import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONTINUOUS_BACKUP_OFFSET_UPDATE_INTERVAL_MS;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONTINUOUS_BACKUP_OFFSET_UPDATE_SIZE_THRESHOLD;
import static org.apache.hadoop.hbase.backup.replication.ContinuousBackupReplicationEndpoint.ONE_DAY_IN_MILLISECONDS;
import static org.apache.hadoop.hbase.mapreduce.WALPlayer.IGNORE_EMPTY_FILES;
import static org.apache.hadoop.hbase.mapreduce.WALPlayer.IGNORE_MISSING_FILES;
import static org.apache.hadoop.hbase.replication.regionserver.ReplicationMarkerChore.REPLICATION_MARKER_ENABLED_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestPointInTimeRestore extends IntegrationTestBackupRestoreBase {
  private static final String CLASS_NAME = IntegrationTestPointInTimeRestore.class.getSimpleName();
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestPointInTimeRestore.class);

  protected static final String PITR_TABLE_BASE_NAME = CLASS_NAME + ".pitr.success";
  protected static final String RESTORED_TABLE = "restoredTable";
  protected static final int DEFAULT_NUMBER_OF_TABLES = 2;

  private static Path restoreRootDir;

  @Override
  @BeforeEach
  public void setUp() throws Exception {
    initializeTestParameters();
    BackupTestUtil.enableBackup(conf);
    conf.setBoolean(REPLICATION_MARKER_ENABLED_KEY, true);
    conf.setBoolean(IGNORE_EMPTY_FILES, true);
    conf.setBoolean(IGNORE_MISSING_FILES, true);
    conf.setLong(CONTINUOUS_BACKUP_OFFSET_UPDATE_INTERVAL_MS, 2000L); // 2 seconds
    conf.setLong(CONTINUOUS_BACKUP_OFFSET_UPDATE_SIZE_THRESHOLD, 1024L); // 1 KB

    regionServerCount = 1;
    rowsInIteration = 100;
    numTables = DEFAULT_NUMBER_OF_TABLES;

    LOG.info("Initializing cluster with {} region server(s)", regionServerCount);
    util.initializeCluster(regionServerCount);
    LOG.info("Cluster initialized and ready");

    backupRootDir = util.getDataTestDirOnTestFS() + Path.SEPARATOR + DEFAULT_BACKUP_ROOT_DIR;
    LOG.info("The backup root directory is: {}", backupRootDir);
    createAndSetBackupWalDir();
    fs = FileSystem.get(conf);

    restoreRootDir = BackupUtils.getTmpRestoreOutputDir(FileSystem.get(conf), conf);
    LOG.info("kevin: restoreRootDir is: {}", restoreRootDir);
  }

  @Test
  public void testPointInTimeRestore() throws Exception {
    LOG.info("Running Point-In-Time-Restore integration test");
    try (Connection conn = util.getConnection(); BackupAdmin client = new BackupAdminImpl(conn)) {

      runPitrFailureFromUsingNonContinuousFullBackup(client);

      setEnvironmentEdgeToNumDaysAgo(30);

      createTables(PITR_TABLE_BASE_NAME);
      List<TableName> tables = Arrays.stream(tableNames).toList();
      List<String> backupIds = new ArrayList<>();

      // Add rows to the table
      loadDataIntoTables(tables);
      LinkedHashMap<TableName, Integer> preFullBackupRowCounts = getExpectedTargetTableRowCounts(tables);

      // Create a full backup with continuous backup enabled
      BackupRequest fullBackupRequest = createFullBackupRequest(tables, true);
      String fullBackupId = backup(fullBackupRequest, client, backupIds);
      LOG.info("Finished full backup on tables {} with backup ID: {}", tables, fullBackupId);

      setEnvironmentEdgeToNumDaysAgo(25);

      // Add rows to the table
      LOG.info("kevin: Loading more data into table {} after performing PITR", tables.get(0));
      loadDataIntoTables(tables);
      int postFullBackupRowCount = PITRTestUtil.getRowCount(util, tables.get(0));
      LOG.info("kevin: Current row count for table after full backup {} is: {}", tables.get(0),
        postFullBackupRowCount);

      int numDaysAgo = 20;
      setEnvironmentEdgeToNumDaysAgo(numDaysAgo);

      runPitrValidationOnlyModeTestCase(client, tables);
      runPitrFailureFromUsingTimeBeforeOldestBackup(client, tables);
      runPitrFailureFromUsingDateAfterRetentionWindow(client, tables);
      runSuccessfulPitrAndVerifyRowCount(client, tables, preFullBackupRowCounts, 26);

      // The original tables still have all of their rows
      int rowCountMultiplier = 2;
      verifyExpectedSourceTableRowCounts(tables, rowCountMultiplier);

      for (int i = 1; i <= 3; i++) {
        // Move time "two days closer". Ex. 20 days ago -> 18
        // Add data to the tables at this point in time.
        numDaysAgo = numDaysAgo - 2;
        setEnvironmentEdgeToNumDaysAgo(numDaysAgo);
        loadDataIntoTables(tables);
        LinkedHashMap<TableName, Integer> preIncrementalBackupRowCount
          = getExpectedTargetTableRowCounts(tables);

        waitForCheckpointToUpdateForAllTables(conn, tables);

        LOG.info("kevin: Creating incremental backup number {} for tables: {}", i, tables);
        BackupRequest request = createIncrementalBackupRequest(tables, true);
        String incrementalBackupId = backup(request, client, backupIds);
        LOG.info("kevin: Created incremental backup number {} on tables {} with ID: {}", i, tables,
          incrementalBackupId);

        // Move time another "two days closer". Ex. 18 days ago -> 16
        // Add data to the tables at this point in time.
        setEnvironmentEdgeToNumDaysAgo(numDaysAgo - 2);
        loadDataIntoTables(tables);

        // Restore tables to how they were "one day ago". Ex. Restore to 17 days ago
        // The most recently added data should not be in these restored tables
        runSuccessfulPitrAndVerifyRowCount(client, tables, preIncrementalBackupRowCount,
          numDaysAgo - 1);

        // The original table still has all of its rows
        rowCountMultiplier = rowCountMultiplier + 2;
        verifyExpectedSourceTableRowCounts(tables, rowCountMultiplier);
      }
    }
  }

  // The PITR fails due to one table not having continuous backup enabled
  private void runPitrFailureFromUsingNonContinuousFullBackup(BackupAdmin client) throws Exception {
    setEnvironmentEdgeToNumDaysAgo(50);

    // This table will be backed up via a non-continuous full backup
    TableName nonContinuousTableName =
      TableName.valueOf(CLASS_NAME + ".non-continuous.pitr.failure");
    createTable(nonContinuousTableName);

    loadData(nonContinuousTableName, rowsInIteration);

    // Create a full non-continuous backup
    BackupRequest fullBackupRequest =
      createFullBackupRequest(List.of(nonContinuousTableName), false);
    String backupId = backup(fullBackupRequest, client, null);

    setEnvironmentEdgeToNumDaysAgo(40);
    loadData(nonContinuousTableName, rowsInIteration);

    // Run a PITR that should fail since the full backup is not continuous
    PointInTimeRestoreRequest pitrRequest = createPitrRequest(List.of(nonContinuousTableName),
      List.of(TableName.valueOf("nonContinuousRestoredTable")),
      System.currentTimeMillis() - 41 * ONE_DAY_IN_MILLISECONDS, false);
    LOG.info("kevin: Running Point-In-Time-Restore on a non-continuous full backup. "
      + "Expecting a failure to occur");
    try {
      pointInTimeRestore(pitrRequest, client);
      fail(
        "An IOException should have occurred due to running PITR on a non-continuous full backup");
    } catch (IOException e) {
      String expectedMsg = String.format(
        "Continuous Backup is not enabled for the following tables: %s", nonContinuousTableName);
      assertTrue(e.getMessage().contains(expectedMsg),
        getIOExceptionAssertionFailureMsg(expectedMsg, e));
      LOG.info("kevin: Got expected IOException after PITR on a non-continuous full backup");
    }

    // These backups are no longer needed
    LOG.info("kevin: Deleting full backup with ID: {}", backupId);
    delete(new String[] { backupId }, client);
  }

  private void runPitrValidationOnlyModeTestCase(BackupAdmin client, List<TableName> tables)
    throws IOException {
    List<TableName> restoredTables = getTargetTablesForPitrRestore(tables);

    // Create request for PITR in validation-only mode
    PointInTimeRestoreRequest pitrRequest = createPitrRequest(tables, restoredTables,
      System.currentTimeMillis() - 26 * ONE_DAY_IN_MILLISECONDS, true);

    // Run PITR in validation-only mode
    LOG.info("kevin: Running Point-In-Time-Restore in validation-only mode");
    pointInTimeRestore(pitrRequest, client);
    LOG.info("kevin: Finished running Point-In-Time-Restore in validation-only mode");
  }

  private void runPitrFailureFromUsingTimeBeforeOldestBackup(BackupAdmin client,
    List<TableName> tables) {
    List<TableName> restoredTables = getTargetTablesForPitrRestore(tables);

    // Create bad PITR request due to date being before the oldest backup
    long restoreToDatetime = System.currentTimeMillis() - 31 * ONE_DAY_IN_MILLISECONDS;
    PointInTimeRestoreRequest pitrRequest = createPitrRequest(tables, restoredTables,
      restoreToDatetime, false);

    // Run actual PITR
    LOG.info("kevin: Performing faulty Point-In-Time-Restore on table {} due to bad restore date",
      tables);
    try {
      pointInTimeRestore(pitrRequest, client);
      fail("Expected PITR to fail due to bad restore date");
    } catch (IOException e) {
      String expectedMsg = String.format(
        "PITR failed: No valid backup/WALs found for source table %s (target: %s) before time %s",
        tables.get(0), restoredTables.get(0), restoreToDatetime);
      assertTrue(e.getMessage().contains(expectedMsg),
        getIOExceptionAssertionFailureMsg(expectedMsg, e));
      LOG.info(
        "kevin: Got expected IOException after attempting PITR using a date before the oldest backup");
    }
  }

  private void runPitrFailureFromUsingDateAfterRetentionWindow(BackupAdmin client,
    List<TableName> tables) {
    List<TableName> restoredTables = getTargetTablesForPitrRestore(tables);

    // Create bad PITR request due to date being before the oldest backup
    long requestedRecoveryTime = System.currentTimeMillis() + ONE_DAY_IN_MILLISECONDS;
    PointInTimeRestoreRequest pitrRequest =
      createPitrRequest(tables, restoredTables, requestedRecoveryTime, false);

    // Run actual PITR
    LOG.info("kevin: Performing faulty Point-In-Time-Restore on table {} due to restore date "
      + "after retention window", tables);
    try {
      pointInTimeRestore(pitrRequest, client);
      fail("Expected PITR to fail due to using date after retention window");
    } catch (IOException e) {
      String expectedMsg = String.format(
        "Requested recovery time (%s) is in the future", requestedRecoveryTime);
      assertTrue(e.getMessage().contains(expectedMsg),
        getIOExceptionAssertionFailureMsg(expectedMsg, e));
      LOG.info("kevin: Got expected IOException after attempting PITR using a date after the "
        + "retention window");
    }
  }

  private void runSuccessfulPitrAndVerifyRowCount(BackupAdmin client, List<TableName> tables,
    LinkedHashMap<TableName, Integer> expectedPostPitrRowCounts, int toDaysAgo) throws IOException {
    List<TableName> restoredTables = getTargetTablesForPitrRestore(tables);

    // Create PITR request
    PointInTimeRestoreRequest pitrRequest = createPitrRequest(tables, restoredTables,
      System.currentTimeMillis() - toDaysAgo * ONE_DAY_IN_MILLISECONDS, false);

    // Run PITR
    LOG.info("kevin: Performing Point-In-Time-Restore on tables {} and restoring to tables {}",
      tables, restoredTables);
    pointInTimeRestore(pitrRequest, client);
    LOG.info("kevin: Finished Point-In-Time-Restore on tables {} to tables {}", tables,
      restoredTables);

    // Verify row count post-PITR
    LOG.info("Verifying row counts for restored tables {} after PITR", restoredTables);
    verifyExpectedRowCountInAllTables(expectedPostPitrRowCounts);
  }

  private PointInTimeRestoreRequest createPitrRequest(List<TableName> tableNames,
    List<TableName> restoredTables, long restoreToDatetime, boolean isCheckOnly) {
    PointInTimeRestoreRequest.Builder pitrBuilder = new PointInTimeRestoreRequest.Builder();
    return pitrBuilder.withBackupRootDir(backupRootDir)
      .withFromTables(tableNames.toArray(new TableName[0]))
      .withToTables(restoredTables.toArray(new TableName[0])).withToDateTime(restoreToDatetime)
      .withRestoreRootDir(restoreRootDir.toString()).withCheck(isCheckOnly).withOverwrite(true)
      .build();
  }

  private void setEnvironmentEdgeToNumDaysAgo(int numDaysAgo) {
    EnvironmentEdgeManager
      .injectEdge(() -> System.currentTimeMillis() - numDaysAgo * ONE_DAY_IN_MILLISECONDS);
    LOG.info("kevin: Finished setting to {} days ago", numDaysAgo);
    LOG.info("kevin: EnvironmentEdgeManager current time: {}",
      EnvironmentEdgeManager.currentTime());
  }

  private void loadDataIntoTables(List<TableName> tables) throws IOException {
    TableName tableName;
    int numRows;
    for (int i = 0; i < tables.size(); i++) {
      tableName = tables.get(i);
      // Potentially vary row count for each table
      if (rowsInIteration > numTables) {
        // Table "i" gets 1/(i + 1) of table 0's row count
        numRows = rowsInIteration / (i + 1);
      } else {
        // If numTables > rowsInIteration, then "rowsInIteration / (i + 1)" could be 0 due to
        // integer division. Give all tables the same number of rows instead
        numRows = rowsInIteration;
      }
      LOG.info("kevin: Loading {} rows into table {}", numRows, tableName);
      loadData(tableName, numRows);
    }
  }

  private void verifyExpectedRowCountInAllTables(
    LinkedHashMap<TableName, Integer> expectedCurrentRowCounts) throws IOException {
    TableName table;
    int expectedNumRows;
    String msg;

    for (Map.Entry<TableName, Integer> entry : expectedCurrentRowCounts.entrySet() ) {
      table = entry.getKey();
      expectedNumRows = entry.getValue();
      msg = String.format("Expected table %s to have %s rows", table, expectedNumRows);
      LOG.info("kevin: Verifying table {} has {} rows", table, expectedNumRows);
      assertEquals(expectedNumRows, PITRTestUtil.getRowCount(util, table), msg);
    }
  }

  private void verifyExpectedSourceTableRowCounts(List<TableName> tables, int rowCountMultiplier) throws IOException {
    LinkedHashMap<TableName, Integer> expectedSourceTableRowCounts
      = getExpectedSourceTableRowCounts(tables, rowCountMultiplier);
    verifyExpectedRowCountInAllTables(expectedSourceTableRowCounts);
  }

  private LinkedHashMap<TableName, Integer> getTableRowCounts(List<TableName> tables)
    throws IOException {
    int numRows;
    LinkedHashMap<TableName, Integer> tableRowCounts = new LinkedHashMap<>();
    for (TableName table : tables) {
      numRows = PITRTestUtil.getRowCount(util, table);
      tableRowCounts.put(table, numRows);
      LOG.info("kevin: Current row count for table {} is: {}", table, numRows);
    }
    return tableRowCounts;
  }

  private LinkedHashMap<TableName, Integer> getExpectedTargetTableRowCounts(List<TableName> tables)
    throws IOException {
    List<TableName> targetTables = getTargetTablesForPitrRestore(tables);
    LinkedHashMap<TableName, Integer> expectedRowCounts = new LinkedHashMap<>();
    for (int i = 0; i < tables.size(); i++) {
      expectedRowCounts.put(targetTables.get(i),
        PITRTestUtil.getRowCount(util, tables.get(i)));
    }
    return expectedRowCounts;
  }

  private LinkedHashMap<TableName, Integer> getExpectedSourceTableRowCounts(List<TableName> tables,
    int rowCountMultiplier) {
    int expectedRowCount = rowsInIteration * rowCountMultiplier;
    LinkedHashMap<TableName, Integer> expectedRowCounts = new LinkedHashMap<>();
    for (int i = 0; i < tables.size(); i++) {
      if (rowsInIteration > numTables) {
        // Table "i" is expected to have 1/(i + 1) of table 0's row count
        expectedRowCount = rowsInIteration / (i + 1) * rowCountMultiplier;
      }
      expectedRowCounts.put(tables.get(i), expectedRowCount);
    }
    return expectedRowCounts;
  }

  protected void waitForCheckpointToUpdateForAllTables(Connection conn, List<TableName> tables)
    throws IOException, InterruptedException {
    long waitStartTime = System.currentTimeMillis();
    for (TableName tableName : tables) {
      LOG.info("Waiting for checkpoint timestamp to update for tables {}", tables);
      try (Table tableConn = conn.getTable(tableName)) {
        long latestPutTimestamp = getLatestPutTimestamp(tableConn);
        waitForCheckpointTimestampsToUpdate(conn, latestPutTimestamp, tableName);
      }
    }
    LOG.info("Done waiting for checkpoint timestamps to update for tables {}. "
        + "Total wait time: {} seconds",
      tables, (System.currentTimeMillis() - waitStartTime) / 1000);
  }

  protected List<TableName> getTargetTablesForPitrRestore(List<TableName> backedUpTables) {
    List<TableName> restoredTables = new ArrayList<>(List.of());
    for (int i = 0; i < backedUpTables.size(); i++) {
      restoredTables.add(TableName.valueOf(RESTORED_TABLE + i));
    }
    return restoredTables;
  }

  protected String getIOExceptionAssertionFailureMsg(String expectedMsg, IOException e) {
    return "Expected IOException to occur with the following message:\n" + expectedMsg
      + ".\n\nInstead got:\n" + e.getMessage();
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    return 0;
  }
}
