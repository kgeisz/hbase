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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
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

//      runPitrFailureFromUsingNonContinuousFullBackup(client);

      setEnvironmentEdgeToNumDaysAgo(30);

      // TODO:
      //  test deleting WALs
      //  test with custom backup location
      //  verify ignoring incomplete/failed backups (how do I force a failure?)
      createTables(PITR_TABLE_BASE_NAME);
      List<TableName> tables = Arrays.stream(tableNames).toList();
      List<String> backupIds = new ArrayList<>();

      // Add rows to the table
      loadRandomDataIntoTables(tables);
      LinkedHashMap<TableName, Integer> expectedTargetTableRowCounts = getExpectedTargetTableRowCounts(tables);

      // Create a full backup with continuous backup enabled
      BackupRequest fullBackupRequest = createFullBackupRequest(tables, true);
      String fullBackupId = backup(fullBackupRequest, client, backupIds);
      LOG.info("Finished full backup on tables {} with backup ID: {}", tables, fullBackupId);

      setEnvironmentEdgeToNumDaysAgo(25);

      // Add rows to the table
      LOG.info("kevin: Loading more data into table {} after performing PITR", tables.get(0));
      loadRandomDataIntoTables(tables);
      int postFullBackupRowCount = PITRTestUtil.getRowCount(util, tables.get(0));
      LOG.info("kevin: Current row count for table after full backup {} is: {}", tables.get(0),
        postFullBackupRowCount);

      int numDaysAgo = 20;
      setEnvironmentEdgeToNumDaysAgo(numDaysAgo);

//      runPitrValidationOnlyModeTestCase(client, tables);
//      runPitrFailureFromUsingTimeBeforeOldestBackup(client, tables);
//      runPitrFailureFromUsingDateAfterRetentionWindow(client, tables);
      runSuccessfulPitrAndVerifyRowCount(client, tables, expectedTargetTableRowCounts, 26);

      // The original tables still have all of their rows
      int rowCountMultiplier = 2;
      verifyExpectedSourceTableRowCounts(tables, rowCountMultiplier);

      for (int i = 1; i <= 3; i++) {
        // Move time "two days closer". Ex. 20 days ago -> 18
        // Add data to the tables at this point in time.
        numDaysAgo = moveTwoDaysCloserAndLoadData(tables, numDaysAgo);
        expectedTargetTableRowCounts = getExpectedTargetTableRowCounts(tables);

        waitForCheckpointToUpdateForAllTables(conn, tables);

        LOG.info("kevin: Creating incremental backup number {} for tables: {}", i, tables);
        BackupRequest request = createIncrementalBackupRequest(tables, true);
        String incrementalBackupId = backup(request, client, backupIds);
        LOG.info("kevin: Created incremental backup number {} on tables {} with ID: {}", i, tables,
          incrementalBackupId);

        // Move time another "two days closer". Ex. 18 days ago -> 16
        // Add data to the tables at this point in time.
        numDaysAgo = moveTwoDaysCloserAndLoadData(tables, numDaysAgo);

        // Restore tables to how they were "one day ago"
        // Ex. Current time is "16 days ago"; restore to 17 days ago
        // The most recently added data should not be in these restored tables
        runSuccessfulPitrAndVerifyRowCount(client, tables, expectedTargetTableRowCounts,
          numDaysAgo + 1);

        // The original table still has all of its rows
        rowCountMultiplier = rowCountMultiplier + 2;
        verifyExpectedSourceTableRowCounts(tables, rowCountMultiplier);
      }

      // Setting to 6 days ago after the 3 iterations above
      numDaysAgo = numDaysAgo - 2;
      setEnvironmentEdgeToNumDaysAgo(numDaysAgo);

      expectedTargetTableRowCounts = getExpectedTargetTableRowCounts(tables);
      LinkedHashMap<TableName, Integer> sourceTableRowCountsPreRowDelete = getTableRowCounts(tables);

      // Delete a row from each table and save the deleted row
      List<byte[]> deletedRowFromEachSourceTable = deleteFirstRowFromEachTable(conn, tables);
      verifyRowDeletedInEachTable(
        conn, tables, deletedRowFromEachSourceTable, sourceTableRowCountsPreRowDelete);

      waitForCheckpointToUpdateForAllTables(conn, tables);

      // Set to 4 days ago
      numDaysAgo = numDaysAgo - 2;
      setEnvironmentEdgeToNumDaysAgo(numDaysAgo);

      // Run PITR and verify the deleted row has been restored in each table
      // A row was deleted from each table "6 days ago". Restore to "7 days ago" and verify
      // each restored table has the deleted row
      List<TableName> restoredTables
        = runSuccessfulPitrAndVerifyRowCount(client, tables, expectedTargetTableRowCounts, numDaysAgo + 3);
      verifyEachRestoredTableHasDeletedRow(conn, restoredTables, deletedRowFromEachSourceTable);
    }
  }

  // The PITR fails due to one table not having continuous backup enabled
  private void runPitrFailureFromUsingNonContinuousFullBackup(BackupAdmin client) throws Exception {
    setEnvironmentEdgeToNumDaysAgo(50);

    // This table will be backed up via a non-continuous full backup
    TableName nonContinuousTableName =
      TableName.valueOf(CLASS_NAME + ".non-continuous.pitr.failure");
    createTable(nonContinuousTableName);

    loadRandomData(nonContinuousTableName, rowsInIteration);

    // Create a full non-continuous backup
    BackupRequest fullBackupRequest =
      createFullBackupRequest(List.of(nonContinuousTableName), false);
    String backupId = backup(fullBackupRequest, client, null);

    setEnvironmentEdgeToNumDaysAgo(40);
    loadRandomData(nonContinuousTableName, rowsInIteration);

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

  private List<TableName> runSuccessfulPitrAndVerifyRowCount(BackupAdmin client, List<TableName> tables,
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

    return restoredTables;
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

  private int moveTwoDaysCloserAndLoadData(List<TableName> tables, int numDaysAgo)
    throws IOException {
    numDaysAgo = numDaysAgo - 2;
    setEnvironmentEdgeToNumDaysAgo(numDaysAgo);
    loadRandomDataIntoTables(tables);
    return numDaysAgo;
  }

  private void loadRandomDataIntoTables(List<TableName> tables) throws IOException {
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
      loadRandomData(tableName, numRows);
    }
  }

  private boolean rowExistsInTable(Table table, byte[] rowKey) throws IOException {
    return table.exists(new Get(rowKey));
  }

  private List<byte[]> deleteFirstRowFromEachTable(Connection conn, List<TableName> tables)
    throws IOException {
    List<byte[]> deletedRows = new ArrayList<>();
    for (TableName tableName : tables) {
      try (Table table = conn.getTable(tableName)) {
        deletedRows.add(deleteFirstRow(table));
      }
    }
    return deletedRows;
  }

  private byte[] deleteFirstRow(Table table) throws IOException {
    byte[] firstRowKey = null;

    // Scan for exactly one row, returning only keys
    Scan scan = new Scan()
      .setOneRowLimit()
      .setFilter(new KeyOnlyFilter());

    try (ResultScanner scanner = table.getScanner(scan)) {
      Result result = scanner.next();
      if (result != null && !result.isEmpty()) {
        firstRowKey = result.getRow();
      }
    }

    // Delete the row if the table is not empty
    if (firstRowKey != null) {
      Delete delete = new Delete(firstRowKey);
      table.delete(delete);
    }

    return firstRowKey;
  }

  private void verifyRowDeletedInEachTable(Connection conn, List<TableName> tables,
    List<byte[]> deletedRows, LinkedHashMap<TableName, Integer> preDeleteRowCounts) throws IOException {
    LinkedHashMap<TableName, Integer> actualSourceRowCountsPostDelete = getTableRowCounts(tables);
    Iterator<byte[]> deletedRowsIterator = deletedRows.iterator();
    for (Map.Entry<TableName, Integer> entry : actualSourceRowCountsPostDelete.entrySet()) {
      TableName tableName = entry.getKey();
      Integer actualRowCount = entry.getValue();
      Integer expectedRowCount = preDeleteRowCounts.get(tableName) - 1;
      byte[] deletedRow = deletedRowsIterator.next();
      String msg = String.format(
        "Expected actual row count for table %s to be %d, got %d instead",
        tableName, expectedRowCount, actualRowCount
      );
      assertEquals(actualRowCount, expectedRowCount, msg);
      try (Table table = conn.getTable(tableName)) {
        msg = String.format("Expected row to be deleted from table %s: %s",
          table, Bytes.toString(deletedRow));
        assertFalse(rowExistsInTable(table, deletedRow), msg);
      }
    }
  }

  private void verifyEachRestoredTableHasDeletedRow(Connection conn, List<TableName> restoredTables,
    List<byte[]> deletedRows) throws IOException {
    assertEquals(restoredTables.size(), deletedRows.size());
    Iterator<TableName> restoredTablesIterator = restoredTables.iterator();
    Iterator<byte[]> deletedRowsIterator = deletedRows.iterator();
    while (restoredTablesIterator.hasNext() && deletedRowsIterator.hasNext()) {
      TableName restoredTable = restoredTablesIterator.next();
      byte[] restoredRow = deletedRowsIterator.next();
      try (Table table = conn.getTable(restoredTable)) {
        String msg = String.format("Expected row to exist in table %s: %s",
          table, Bytes.toString(restoredRow));
        assertTrue(rowExistsInTable(table, restoredRow), msg);
      }
    }
  }

  private void verifyExpectedRowCountInAllTables(
    LinkedHashMap<TableName, Integer> expectedCurrentRowCounts) throws IOException {
    TableName table;
    int expectedNumRows;
    String msg;

    for (Map.Entry<TableName, Integer> entry : expectedCurrentRowCounts.entrySet()) {
      table = entry.getKey();
      expectedNumRows = entry.getValue();
      msg = String.format("Expected table %s to have %s rows", table, expectedNumRows);
      LOG.info("kevin: Verifying table {} has {} rows", table, expectedNumRows);
      assertEquals(expectedNumRows, PITRTestUtil.getRowCount(util, table), msg);
    }
  }

  private void verifyExpectedSourceTableRowCounts(List<TableName> tables, int rowCountMultiplier) throws IOException {
    LinkedHashMap<TableName, Integer> expectedSourceTableRowCounts
      = calculateExpectedSourceTableRowCounts(tables, rowCountMultiplier);
    verifyExpectedRowCountInAllTables(expectedSourceTableRowCounts);
  }

  /**
   * Gets the current row count for each table in the provided list
   * @param tables - tables we want to get the row count for
   * @return       - Map where keys are each table and the values are each table's row count
   * @throws IOException
   */
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

  /**
   * Gets the current row count of each source table and maps them to the name of each target table.
   * This acts as the expected row count for each target table after the source tables have been
   * mutated and a PITR has been run.
   * @param tables - tables we want to get the current row count for so these values can be
   *                 verified later after PITR
   * @return       - Map of restored tables to expected row count
   * @throws IOException
   */
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

  /**
   * Uses a row count multiplier to calculate how many rows each source table in a list should have
   * at a certain time
   * @param tables             - source tables
   * @param rowCountMultiplier - factor used to calculare each table's row count
   * @return
   */
  private LinkedHashMap<TableName, Integer> calculateExpectedSourceTableRowCounts(List<TableName> tables,
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
        long latestMutationTimestamp = getLatestMutationTimestamp(tableConn);
        waitForCheckpointTimestampsToUpdate(conn, latestMutationTimestamp, tableName);
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
