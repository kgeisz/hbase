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

import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
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
  public void testReadReplicaCluster() throws IOException, InterruptedException {
    LOG.info("kevin: start of testReadReplicaCluster()");
    // Create a table. The replica cluster won't have this table until refresh_meta is run.
    final String table1 = "testTable1";
    attemptCreateOnReplicaCluster(utilB, table1);
    createTableOnActiveCluster(utilA, table1);
    assertEquals("The active cluster should have a table called " + table1,
      table1, utilA.getAdmin().listTableNames()[0].getNameAsString());
    assertTrue("The read replica cluster should not have any tables yet",
      Arrays.stream(utilB.getAdmin().listTableNames()).toList().isEmpty());
    refreshMeta(utilB);
    assertFalse("The replica cluster should now have a table called " + table1,
      Arrays.stream(utilB.getAdmin().listTableNames()).toList().isEmpty());

    // Add data to the active cluster
    final String row1 = "row1";
    try (Table activeTable = connectionA.getTable(TableName.valueOf(table1))) {
      // Add data to the active cluster
      Put put = new Put(Bytes.toBytes(row1));
      put.addColumn(COLUMN_FAMILY, QUALIFIER, Bytes.toBytes("1"));
      activeTable.put(put);
    }

    // Verify new data cannot be put on the replica cluster
    attemptPutOnReplicaCluster(utilB, table1);

    Result result = getRow(connectionA, table1, row1);
    assertFalse("The Get result should not be empty on the active cluster since data was inserted "
      + "for row " + row1, result.isEmpty());
    utilA.getAdmin().flush(TableName.valueOf(table1));

    result = getRow(connectionB, table1, row1);
    assertTrue(
      "The Get result should be empty on the replica cluster since it has not been refreshed",
      result.isEmpty());

    // The replica cluster should have the same data as the active cluster after refresh
    refreshMeta(utilB);
    refreshHFiles(utilB);

    result = getRow(connectionB, table1, row1);
    assertFalse(
      "The Get result should not be empty on the replica cluster since it has been refreshed",
      result.isEmpty());

    // TODO - switch the active cluster and the replica cluster

    // Dynamically switch the active cluster to read-only mode and verify it no longer supports
    // puts and table creations
    confA.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    utilA.notifyConfigurationObservers(clusterA);
    attemptPutOnReplicaCluster(utilA, table1);
    final String table2 = "testTable2";
    attemptCreateOnReplicaCluster(utilA, table2);

    confB.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    utilB.notifyConfigurationObservers(clusterB);
    createTableOnActiveCluster(utilB, table2);

    // try (Table replicaTable = replicaConn.getTable(TableName.valueOf(TABLE_NAME))) {
    // Get get = new Get(row1);
    // get.addColumn(COLUMN_FAMILY, QUALIFIER);
    // Result result = replicaTable.get(get);
    // LOG.info("kevin: replica cluster get result = {}", result);
    // String s = "e";
    // }

    String s = "e";
  }
}
