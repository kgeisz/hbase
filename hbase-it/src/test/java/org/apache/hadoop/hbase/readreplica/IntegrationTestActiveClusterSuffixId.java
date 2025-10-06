package org.apache.hadoop.hbase.readreplica;

import org.apache.hadoop.hbase.ActiveClusterSuffix;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTests.class)
public class IntegrationTestActiveClusterSuffixId extends IntegrationTestReadReplicaBase {
  protected static final Logger LOG =
    LoggerFactory.getLogger(IntegrationTestActiveClusterSuffixId.class);

  // Checks for the correct active cluster ID and suffix in the active cluster file
  public void validateActiveClusterSuffixFile(String clusterId, String suffix) throws IOException {
    ActiveClusterSuffix activeClusterSuffix = FSUtils.getActiveClusterSuffix(fs, rootDir);
    String expectedIdAndSuffix = clusterId + ":" + suffix;
    assertEquals("Expected the active cluster file to have the following cluster ID and "
        + "suffix: " + expectedIdAndSuffix, expectedIdAndSuffix,
      activeClusterSuffix.getActiveClusterSuffix());
  }

  @Test
  public void testActiveClusterSuffixIdFile() throws IOException, InterruptedException {
    // The base integration test class starts the system with Cluster A as the active cluster and
    // Cluster B as the replica cluster
    validateActiveClusterSuffixFile(clusterA.getMaster().getClusterId(), "");

    // Create a table to prove Cluster A is the active cluster
    createTableOnActiveCluster(utilA, Bytes.toBytes("testTable1"));

    // Purposely fail to create a table on Cluster B to prove it is the replica cluster
    attemptCreateOnReplicaCluster(utilB, Bytes.toBytes("badCreate1"));

    // Put Cluster A in read-only mode
    confA.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, true);
    utilA.notifyConfigurationObservers(clusterA);

    // Verify a table can no longer be created on Cluster A since it is now in read-only mode
    attemptCreateOnReplicaCluster(utilA, Bytes.toBytes("badCreate2"));

    // Make Cluster B the active cluster
    confB.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    utilB.notifyConfigurationObservers(clusterB);

    // Verify a table can now be created on Cluster B since it is the active cluster
    createTableOnActiveCluster(utilB, Bytes.toBytes("testTable2"));

    // Verify the active cluster ID file has been updated with Cluster B's meta table suffix
    validateActiveClusterSuffixFile(clusterB.getMaster().getClusterId(), TEST_META_TABLE_SUFFIX);
  }
}
