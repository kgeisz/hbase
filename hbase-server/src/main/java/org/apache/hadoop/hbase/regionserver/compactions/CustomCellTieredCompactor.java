package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.regionserver.HStore;
import java.util.List;

public class CustomCellTieredCompactor extends DateTieredCompactor {
  public CustomCellTieredCompactor(Configuration conf, HStore store) {
    super(conf, store);
  }

  @Override
  protected void decorateCells(List<Cell> cells) {
    byte[] lastKey = null;
    for(Cell cell : cells) {
      KeyValue kv = new KeyValue(cell);
    }
  }

}
