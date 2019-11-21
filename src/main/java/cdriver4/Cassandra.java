package cdriver4;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Cassandra {
  private final ClusterManager clusterManager;
  private final CqlSession session;

  public Cassandra(List<String> addrs, int port) {
    clusterManager = new ClusterManager();
    session = clusterManager.getSession(addrs, port);
  }

  public ResultSet directExecute(String query) {
    return session.execute(query);
  }

  public ResultSet directExecute(Statement statement) {
    return session.execute(statement);
  }

  public ResultSet get(Get get) {
    PreparedStatement prepared = prepareSelect(get);
    BoundStatement bound = bindSelect(prepared, get);
    // TODO: error handling
    return session.execute(bound);
  }

  public ResultSet put(Put put) {
    PreparedStatement prepared = prepareInsert(put);
    BoundStatement bound = bindInsert(prepared, put);
    // TODO: error handling
    return session.execute(bound);
  }

  public ResultSet updates(Update update) {
    PreparedStatement prepared = prepareUpdate(update);
    BoundStatement bound = bindUpdate(prepared, update);
    // TODO: error handling
    return session.execute(bound);
  }

  private PreparedStatement prepareSelect(Get get) {
    SelectFrom selectFrom = selectFrom(get.getKeyspace(), get.getTable());

    // TODO: set projections
    Select select = selectFrom.all();

    // set predicates
    List<Relation> relations = new ArrayList<Relation>();
    get.getPartitionKeyNames()
        .forEach(k -> relations.add(Relation.column(k).isEqualTo(bindMarker())));
    get.getClusteringKeyNames()
        .forEach(k -> relations.add(Relation.column(k).isEqualTo(bindMarker())));

    String queryString = select.where(relations).asCql();

    // make a statement
    PreparedStatement prepared = null;
    // PreparedStatement prepared = cache.get(queryString); // recommended to use cache
    if (prepared == null) {
      prepared = session.prepare(queryString);
      // cache.put(queryString, prepared);
    }

    return prepared;
  }

  private BoundStatement bindSelect(PreparedStatement prepared, Get get) {
    BoundStatement bound = prepared.bind();
    int i = 0;

    // bind in the prepared order
    for (String v : get.getPartitionKeyValues()) {
      bound = bound.setString(i++, v);
    }
    for (String v : get.getClusteringKeyValues()) {
      bound = bound.setString(i++, v);
    }

    return bound;
  }

  private PreparedStatement prepareInsert(Put put) {
    InsertInto insert = insertInto(put.getKeyspace(), put.getTable());

    RegularInsert i = null;
    Map<String, Integer> values = put.getValues();
    for (Key k : put.getPartitionKeys()) {
      if (i == null) {
        i = insert.value(k.getName(), bindMarker());
      }
      i = i.value(k.getName(), bindMarker());
    }
    for (Key k : put.getClusteringKeys()) {
      i = i.value(k.getName(), bindMarker());
    }
    for (String k : put.getValues().keySet()) {
      i = i.value(k, bindMarker());
    }

    // TODO: check conditions
    String queryString = i.ifNotExists().asCql();

    // make a statement
    PreparedStatement prepared = null;
    // PreparedStatement prepared = cache.get(queryString); // recommended to use cache
    if (prepared == null) {
      prepared = session.prepare(queryString);
      // cache.put(queryString, prepared);
    }

    return prepared;
  }

  private BoundStatement bindInsert(PreparedStatement prepared, Put put) {
    BoundStatement bound = prepared.bind();
    int i = 0;

    // bind in the prepared order
    for (String v : put.getPartitionKeyValues()) {
      bound = bound.setString(i++, v);
    }
    for (String v : put.getClusteringKeyValues()) {
      bound = bound.setString(i++, v);
    }
    for (Integer v : put.getValues().values()) {
      bound = bound.setInt(i++, (int) v);
    }

    return bound;
  }

  private PreparedStatement prepareUpdate(Update update) {
    UpdateStart us = update(update.getKeyspace(), update.getTable());

    UpdateWithAssignments ua = null;
    for (String k : update.getValues().keySet()) {
      if (ua == null) {
        ua = us.setColumn(k, bindMarker());
      } else {
        ua = ua.setColumn(k, bindMarker());
      }
    }

    List<Relation> relations = new ArrayList<Relation>();
    update
        .getPartitionKeyNames()
        .forEach(k -> relations.add(Relation.column(k).isEqualTo(bindMarker())));
    update
        .getClusteringKeyNames()
        .forEach(k -> relations.add(Relation.column(k).isEqualTo(bindMarker())));

    // TODO: other operators
    List<Condition> conditions = new ArrayList<Condition>();
    update
        .getConditions()
        .forEach((n, v) -> conditions.add(Condition.column(n).isEqualTo(bindMarker())));

    String queryString = ua.where(relations).if_(conditions).asCql();

    // make a statement
    PreparedStatement prepared = null;
    // PreparedStatement prepared = cache.get(queryString); // recommended to use cache
    if (prepared == null) {
      prepared = session.prepare(queryString);
      // cache.put(queryString, prepared);
    }

    return prepared;
  }

  private BoundStatement bindUpdate(PreparedStatement prepared, Update put) {
    BoundStatement bound = prepared.bind();
    int i = 0;

    // bind in the prepared order
    for (Integer v : put.getValues().values()) {
      bound = bound.setInt(i++, (int) v);
    }
    for (String v : put.getPartitionKeyValues()) {
      bound = bound.setString(i++, v);
    }
    for (String v : put.getClusteringKeyValues()) {
      bound = bound.setString(i++, v);
    }
    for (Integer v : put.getConditions().values()) {
      bound = bound.setInt(i++, (int) v);
    }

    return bound;
  }

  public void close() {
    clusterManager.close();
  }
}
