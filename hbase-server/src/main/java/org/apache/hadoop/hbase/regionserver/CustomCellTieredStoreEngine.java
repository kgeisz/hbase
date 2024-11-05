package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.DateTieredStoreEngine;
import org.apache.hadoop.hbase.regionserver.DefaultStoreFileManager;
import org.apache.hadoop.hbase.regionserver.DefaultStoreFlusher;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFileComparators;
import org.apache.hadoop.hbase.regionserver.compactions.CustomCellDateTieredCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.CustomCellTieredCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactor;
import java.io.IOException;
import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY;

public class CustomCellTieredStoreEngine extends DateTieredStoreEngine {

  @Override
  protected void createComponents(Configuration conf, HStore store, CellComparator kvComparator)
    throws IOException {
    conf = new Configuration(conf);
    conf.set(DEFAULT_COMPACTION_POLICY_CLASS_KEY,
      CustomCellDateTieredCompactionPolicy.class.getName());
    createCompactionPolicy(conf, store);
    this.storeFileManager = new DefaultStoreFileManager(kvComparator,
      StoreFileComparators.SEQ_ID_MAX_TIMESTAMP, conf, compactionPolicy.getConf());
    this.storeFlusher = new DefaultStoreFlusher(conf, store);
    this.compactor = new CustomCellTieredCompactor(conf, store);
  }

}
