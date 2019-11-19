package cdriver4;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Update extends Operation {
  private final Map<String, Integer> values; // TODO: other types
  // TODO: conditions

  public Update(String keyspace, String table, List<Key> partitionKeys) {
    this(keyspace, table, partitionKeys, new ArrayList<Key>());
  }

  public Update(String keyspace, String table, List<Key> partitionKeys, List<Key> clusteringKeys) {
    super(keyspace, table, partitionKeys, clusteringKeys);
    values = new LinkedHashMap<>();
  }

  public Update withValue(String column, Integer value) {
    values.put(column, value);
    return this;
  }

  public Map<String, Integer> getValues() {
    return values;
  }
}
