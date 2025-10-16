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

import static org.apache.hadoop.hbase.IntegrationTestingUtility.createPreSplitLoadTestTable;
import static org.apache.hadoop.hbase.backup.BackupRestoreConstants.CONF_CONTINUOUS_BACKUP_WAL_DIR;

import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IntegrationTestBackupBase extends IntegrationTestBase {
  protected static final Logger LOG = LoggerFactory.getLogger(IntegrationTestBackupBase.class);
  protected static final String COLUMN_NAME = "f";
  protected static final String REGION_COUNT_KEY = "regions_per_rs";
  protected static final String REGIONSERVER_COUNT_KEY = "region_servers";
  protected static final int DEFAULT_REGION_COUNT = 10;
  protected static final int DEFAULT_REGIONSERVER_COUNT = 1;

  protected static int regionsCountPerServer;
  protected static int regionServerCount;

  protected static String BACKUP_ROOT_DIR = "backupRootDir";

  protected void enableBackup(Configuration conf) {
    conf.setBoolean(BackupRestoreConstants.BACKUP_ENABLE_KEY, true);
    BackupManager.decorateMasterConfiguration(conf);
    BackupManager.decorateRegionServerConfiguration(conf);
  }

  protected void createAndSetBackupWalDir(IntegrationTestingUtility util, Configuration conf,
    String backupWalDirName) throws IOException {
    Path root = util.getDataTestDirOnTestFS();
    Path backupWalDir = new Path(root, backupWalDirName);
    FileSystem fs = FileSystem.get(conf);
    fs.mkdirs(backupWalDir);
    conf.set(CONF_CONTINUOUS_BACKUP_WAL_DIR, backupWalDir.toString());
    LOG.info(
      "The continuous backup WAL directory has been created and set in the configuration to: {}",
      backupWalDir);
  }

  protected void createTable(TableName tableName) throws Exception {
    long startTime, endTime;

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);

    TableDescriptor desc = builder.build();
    ColumnFamilyDescriptorBuilder cbuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_NAME.getBytes(Charset.defaultCharset()));
    ColumnFamilyDescriptor[] columns = new ColumnFamilyDescriptor[] { cbuilder.build() };
    LOG.info("Creating table {} with {} splits.", tableName,
      regionsCountPerServer * regionServerCount);
    startTime = EnvironmentEdgeManager.currentTime();
    createPreSplitLoadTestTable(util.getConfiguration(), desc, columns, regionsCountPerServer);
    util.waitTableAvailable(tableName);
    endTime = EnvironmentEdgeManager.currentTime();
    LOG.info("Pre-split table created successfully in {}ms.", (endTime - startTime));
  }

  protected void loadData(TableName tableName, int numRows) throws IOException {
    Connection conn = util.getConnection();
    Table t1 = conn.getTable(tableName);
    util.loadRandomRows(t1, Bytes.toBytes(COLUMN_NAME), 100, numRows);
    conn.getAdmin().flush(TableName.valueOf(tableName.getName()));
  }

  protected String backup(BackupRequest request, BackupAdmin client) throws IOException {
    return client.backupTables(request);
  }

  /**
   * Verifies the backup is in a COMPLETE state and therefore has succeeded.
   */
  protected boolean checkSucceeded(String backupId) throws IOException {
    BackupInfo status = getBackupInfo(backupId);
    if (status == null) {
      return false;
    }
    return status.getState() == BackupInfo.BackupState.COMPLETE;
  }

  private BackupInfo getBackupInfo(String backupId) throws IOException {
    try (BackupSystemTable table = new BackupSystemTable(util.getConnection())) {
      return table.readBackupInfo(backupId);
    }
  }
}
