package cdriver4;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Update extends Operation {
  private final Map<String, Integer> values; // TODO: other types
  private final Map<String, Integer> conditions; // TODO: other types and operators

  public Update(String keyspace, String table, List<Key> partitionKeys) {
    this(keyspace, table, partitionKeys, new ArrayList<Key>());
  }

  public Update(String keyspace, String table, List<Key> partitionKeys, List<Key> clusteringKeys) {
    super(keyspace, table, partitionKeys, clusteringKeys);
    values = new LinkedHashMap<>();
    conditions = new LinkedHashMap<>();
  }

  public Update withValue(String column, Integer value) {
    values.put(column, value);
    return this;
  }

  public Map<String, Integer> getValues() {
    return values;
  }

  public Update withCondition(String name, Integer value) {
    conditions.put(name, value);
    return this;
  }

  public Map<String, Integer> getConditions() {
    return conditions;
  }
}
