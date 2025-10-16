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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupAdminImpl;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * This integration test verifies a full backup with --continuous-backup-enabled can be started and
 * a snapshot gets created at the specified path.
 */
@Category(IntegrationTests.class)
public class IntegrationTestSnapshotWithContinuousBackupEnabled extends IntegrationTestBackupBase {
  private static final String CLASS_NAME =
    IntegrationTestSnapshotWithContinuousBackupEnabled.class.getSimpleName();
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestSnapshotWithContinuousBackupEnabled.class);

  private static final String backupWalDirName = CLASS_NAME + "-BackupWalDir";

  @Override
  @Before
  public void setUp() throws Exception {
    util = new IntegrationTestingUtility();
    conf = util.getConfiguration();
    enableBackup(conf);

    regionsCountPerServer = conf.getInt(REGION_COUNT_KEY, DEFAULT_REGION_COUNT);
    // DEFAULT_REGIONSERVER_COUNT is currently 1, but IntegrationTestBackupRestore uses 5
    regionServerCount = conf.getInt(REGIONSERVER_COUNT_KEY, DEFAULT_REGIONSERVER_COUNT);

    LOG.info("Initializing cluster with {} region server", regionServerCount);
    util.initializeCluster(regionServerCount);
    LOG.info("Cluster initialized and ready");

    BACKUP_ROOT_DIR = util.getDataTestDirOnTestFS() + Path.SEPARATOR + BACKUP_ROOT_DIR;
    LOG.info("The backup root directory is: {}", BACKUP_ROOT_DIR);
    createAndSetBackupWalDir(util, conf, backupWalDirName);
  }

  @After
  public void tearDown() throws IOException {
    LOG.info("Cleaning up after test.");
    if (util.isDistributedCluster()) {
      // deleteTablesIfAny();
      LOG.info("Cleaning up after test. Deleted tables");
      // cleanUpBackupDir();
    }
    LOG.info("Restoring cluster.");
    util.restoreCluster();
    LOG.info("Cluster restored.");
  }

  @Test
  public void testSnapshotWithContinuousBackupEnabled() throws Exception {
    try (Connection conn = util.getConnection(); Admin admin = conn.getAdmin();
      BackupAdmin client = new BackupAdminImpl(conn)) {
      TableName tableName = TableName.valueOf(CLASS_NAME + "-table");
      createTable(tableName);
      loadData(tableName, 1000);
      List<TableName> tables = Lists.newArrayList(tableName);
      BackupRequest.Builder builder = new BackupRequest.Builder();
      BackupRequest request = builder.withBackupType(BackupType.FULL).withTableList(tables)
        .withContinuousBackupEnabled(true).withTargetRootDir(BACKUP_ROOT_DIR).build();

      String backupId = backup(request, client);
      assertTrue(checkSucceeded(backupId));

      FileSystem fs = FileSystem.get(conf);
      RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(new Path(BACKUP_ROOT_DIR, backupId), true);
      Path dataManifestPath = null;
      while (fileStatusIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileStatusIterator.next();
        if (fileStatus.getPath().getName().endsWith("data.manifest")) {
          dataManifestPath = fileStatus.getPath();
          LOG.info(
            "Found snapshot manifest for table '{}' at: {}",
            tableName, dataManifestPath);
        }
      }

      if (dataManifestPath == null) {
        fail("Could not find snapshot manifest for table '" + tableName + "'");
      }
    }
  }

  @Override
  public void setUpCluster() throws Exception {

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

  @Test
  public void testCreateSnapshotWithContinuousBackupEnabled() throws Exception {

  }
}
