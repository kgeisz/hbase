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
package org.apache.hadoop.hbase.readreplica;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
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
public class IntegrationTestReadReplicaCluster extends IntegrationTestReadReplicaBase {
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestReadReplicaCluster.class);

  @Test
  public void testReadReplicaCluster() throws Exception {
    final String table1 = "testTable1";
    final String row1 = "row1";

    LOG.info("kevin: creating table on active Cluster A");
    createTableOnActiveCluster(utilA, table1);
    LOG.info("kevin: putting row on active Cluster A");
    putRowOnActiveCluster(utilA, table1, row1);
    LOG.info("kevin: getting row on active Cluster A");
    Result result = getRow(connectionA, table1, row1);
    assertFalse("The Get result should not be empty on the active cluster since data was inserted "
      + "for row " + row1, result.isEmpty());
    utilA.getAdmin().flush(TableName.valueOf(table1));
    LOG.info("kevin: getting PEWorkerGroup threads");
//    runInputScanner();

    scanMetaTable(connectionA);


    LOG.info("kevin: shutting down HBase and Zookeeper miniclusters for active Cluster A");
    utilA.shutDownMiniHBaseAndZKCluster();

    reinitializeStaticMetaTableName(confB);
    reinitializeFirstMetaRegionInfoForClusterB();
    reinitializeMasterRegionDirName(confB);

//    Thread.sleep(60000);
    LOG.info("kevin: starting HBase and Zookeeper miniclusters for replica Cluster B");
    clusterB = utilB.startMiniCluster();
    LOG.info("kevin: done starting replica Cluster B");
    connectionB = utilB.getConnection();

    scanMetaTable(connectionB);

//    assertProperReplicaClusterInitialization(utilB, CLUSTER_B_META_SUFFIX);
//    assertTableDoesNotExistOnReplicaCluster(utilB, table1);
//    LOG.info("kevin: refreshing meta and hfiles for replica Cluster B");
//    refreshMeta(utilB);
//    refreshHFiles(utilB);
//    assertTableExistsOnReplicaCluster(utilB, table1);
//    LOG.info("kevin: shutting down HBase and Zookeeper miniclusters for replica Cluster B");
//    utilB.shutDownMiniHBaseAndZKCluster();
////    LOG.info("kevin: shutting down HBase minicluster for active Cluster B");
////    utilB.shutdownMiniHBaseCluster();
//
//    reinitializeStaticMetaTableName(confA);
//    reinitializeFirstMetaRegionInfoForClusterA();
//    LOG.info("kevin: starting HBase and Zookeeper miniclusters for active Cluster A");
//    clusterA = utilA.startMiniCluster();
//    assertProperActiveClusterInitialization(utilA, CLUSTER_A_META_SUFFIX);









    // OLD STUFF BELOW

    // Prove a table cannot be created on Cluster B since it is the replica cluster
//    LOG.info("kevin: attempting a create command on the replica cluster and expecting a failure");
//    attemptFailedCreateOnReplicaCluster(utilB, table1);
//    LOG.info("kevin: got the expected exception when trying to create a table on the replica cluster");
//
//    // Create a table. Since Cluster B is the replica cluster, it won't have the new table until
//    // refresh_meta has been run
//    LOG.info("kevin: attempting to create a table on the active cluster");
//    createTableOnActiveCluster(utilA, table1);
//    LOG.info("kevin: done attempting to create a table on the active cluster");
//    assertTableExistsOnActiveCluster(utilA, table1);
//    assertTableDoesNotExistOnReplicaCluster(utilB, table1);
//    refreshMeta(utilB);
//    assertTableExistsOnReplicaCluster(utilB, table1);
//    result = getRow(connectionB, table1, row1);
//    assertTrue(
//      "The Get result should be empty on the replica cluster since it has not been refreshed",
//      result.isEmpty());
//
////    final String row1 = "row1";
//
//    // Add data to the active cluster
//    putRowOnActiveCluster(utilA, table1, row1);
//
//    // Verify new data cannot be put on the replica cluster
//    attemptFailedPutOnReplicaCluster(utilB, table1);
//
//    // The active cluster will see the new data, but the replica cluster won't see this data until
//    // it has been refreshed
//    result = getRow(connectionA, table1, row1);
//    assertFalse("The Get result should not be empty on the active cluster since data was inserted "
//      + "for row " + row1, result.isEmpty());
//    utilA.getAdmin().flush(TableName.valueOf(table1));
//    result = getRow(connectionB, table1, row1);
//    assertTrue(
//      "The Get result should be empty on the replica cluster since it has not been refreshed",
//      result.isEmpty());
//
//    // The replica cluster should have the same data as the active cluster after refreshing
//    refreshMeta(utilB);
//    refreshHFiles(utilB);
//    result = getRow(connectionB, table1, row1);
//    assertFalse(
//      "The Get result should not be empty on the replica cluster since it has been refreshed",
//      result.isEmpty());
//
//    // Put Cluster A in read-only mode and verify it no longer supports Puts and Creates
//    confA.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
//    utilA.notifyConfigurationObservers(clusterA);
//    attemptFailedPutOnReplicaCluster(utilA, table1);
//    final String table2 = "testTable2";
//    attemptFailedCreateOnReplicaCluster(utilA, table2);
//
//    // Make Cluster B the new active cluster and create a new table with data
//    confB.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
//    utilB.notifyConfigurationObservers(clusterB);
//    createTableOnActiveCluster(utilB, table2);
//
//    assertTableExistsOnActiveCluster(utilB, table2);
//    assertTableDoesNotExistOnReplicaCluster(utilA, table2);
//    final String row2 = "row2";
//    putRowOnActiveCluster(utilB, table2, row2);
//    utilB.getAdmin().flush(TableName.valueOf(table2));
  }
}
