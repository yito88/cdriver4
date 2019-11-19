package cdriver4;

import java.util.ArrayList;
import java.util.List;

public class Get extends Operation {
  public Get(String keyspace, String table, List<Key> partitionKeys) {
    this(keyspace, table, partitionKeys, new ArrayList<Key>());
  }

  public Get(String keyspace, String table, List<Key> partitionKeys, List<Key> clusteringKeys) {
    super(keyspace, table, partitionKeys, clusteringKeys);
  }
}
