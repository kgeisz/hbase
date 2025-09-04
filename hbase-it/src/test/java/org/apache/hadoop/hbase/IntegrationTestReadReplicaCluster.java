package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;
import static org.apache.hadoop.hbase.HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_META_TABLE_SUFFIX;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for validating the behavior of the Read-Replica feature. The test starts two separate
 * mini-clusters. The active cluster has
 * {@value org.apache.hadoop.hbase.HConstants#HBASE_GLOBAL_READONLY_ENABLED_KEY} set to false,
 * while the replica cluster has this config variable set to true.
 */
@Category(IntegrationTests.class)
public class IntegrationTestReadReplicaCluster extends IntegrationTestBase {
  protected static final Logger LOG = LoggerFactory.getLogger(IntegrationTestReadReplicaCluster.class);
  protected Configuration conf1;
  protected Configuration conf2;
  protected IntegrationTestingUtility util2;
  SingleProcessHBaseCluster cluster1;
  SingleProcessHBaseCluster cluster2;

  @Override
  public void setUpCluster() throws Exception {
    LOG.info("kevin: start setUpCluster");
    // Starting as the active cluster
    util = new IntegrationTestingUtility();
    conf1 = util.getConfiguration();
    conf1.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    cluster1 = util.startMiniCluster();
    LOG.info("kevin: cluster1 ID from master = {}", cluster1.getMaster().getClusterId());

    // Starting as the replica cluster
    conf2 = HBaseConfiguration.create(conf1);
    conf2.setBoolean(HBASE_GLOBAL_READONLY_ENABLED_KEY, false);
    conf2.set(HBASE_META_TABLE_SUFFIX, "2");
    util2 = new IntegrationTestingUtility(conf2);
    cluster2 = util2.startMiniCluster();
    LOG.info("kevin: cluster2 ID from master = {}", cluster2.getMaster().getClusterId());
    LOG.info("kevin: end setUpCluster");
  }

  @Override
  public void cleanUpCluster() throws Exception {
    LOG.info("kevin: start cleanUpCluster");

    LOG.info("kevin: start restoring cluster1");
    util.restoreCluster();
    LOG.info("kevin: end restoring cluster1");

    LOG.info("kevin: start restoring cluster2");
    util2.restoreCluster();
    LOG.info("kevin: end restoring cluster2");

    LOG.info("kevin: end cleanUpCluster");
  }

  @Test
  public void testReadReplicaCluster() {
//    Configuration c1 = cluster1.getConfiguration();
//    assertFalse("The active cluster should have" + HBASE_GLOBAL_READONLY_ENABLED_KEY
//      + "set to false", Boolean.parseBoolean(c1.get(HBASE_GLOBAL_READONLY_ENABLED_KEY)));
//    Configuration c2 = cluster2.getConfiguration();
//    assertTrue("The replica cluster should have" + HBASE_GLOBAL_READONLY_ENABLED_KEY
//      + "set to true", Boolean.parseBoolean(c2.get(HBASE_GLOBAL_READONLY_ENABLED_KEY)));
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
}
