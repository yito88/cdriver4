package cdriver4;

import java.util.ArrayList;
import java.util.List;

public abstract class Operation {
  private final String keyspace;
  private final String table;
  private final List<Key> partitionKeys;
  private final List<Key> clusteringKeys;

  public Operation(String keyspace, String table, List<Key> partitionKeys) {
    this(keyspace, table, partitionKeys, new ArrayList<Key>());
  }

  public Operation(
      String keyspace, String table, List<Key> partitionKeys, List<Key> clusteringKeys) {
    this.keyspace = keyspace;
    this.table = table;
    this.partitionKeys = partitionKeys;
    this.clusteringKeys = clusteringKeys;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public String getTable() {
    return table;
  }

  public List<Key> getPartitionKeys() {
    return partitionKeys;
  }

  public List<Key> getClusteringKeys() {
    return clusteringKeys;
  }

  public List<String> getPartitionKeyNames() {
    List<String> names = new ArrayList<String>();

    partitionKeys.forEach(k -> names.add(k.getName()));

    return names;
  }

  public List<String> getClusteringKeyNames() {
    List<String> names = new ArrayList<String>();

    clusteringKeys.forEach(k -> names.add(k.getName()));

    return names;
  }

  public List<String> getPartitionKeyValues() {
    List<String> values = new ArrayList<String>();

    partitionKeys.forEach(k -> values.add(k.getValue()));

    return values;
  }

  public List<String> getClusteringKeyValues() {
    List<String> values = new ArrayList<String>();

    clusteringKeys.forEach(k -> values.add(k.getValue()));

    return values;
  }
}
