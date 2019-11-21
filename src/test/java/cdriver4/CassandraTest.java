package cdriver4;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CassandraTest {
  private static final String KEYSPACE = "testks";
  private static final String TABLE = "testtbl";
  private static final String PK = "id";
  private static final String C1 = "val1";
  private static final String C2 = "val2";

  private static final List<String> addrs = new ArrayList<String>(Arrays.asList("localhost"));
  private static final int port = 9042;
  private static final String CASSANDRA_VERSION = "3.11.4-SNAPSHOT";

  @BeforeClass
  public static void makeTable() {
    Cassandra cassandra = new Cassandra(addrs, port);

    Drop dropKs = SchemaBuilder.dropKeyspace(KEYSPACE).ifExists();
    CreateKeyspace createKs = SchemaBuilder.createKeyspace(KEYSPACE).withSimpleStrategy(1);
    CreateTable createTbl =
        SchemaBuilder.createTable(KEYSPACE, TABLE)
            .withPartitionKey(PK, DataTypes.TEXT)
            .withColumn(C1, DataTypes.INT)
            .withColumn(C2, DataTypes.INT);

    cassandra.directExecute(dropKs.build());
    cassandra.directExecute(createKs.build());
    cassandra.directExecute(createTbl.build());

    cassandra.close();
  }

  /*
  @AfterClass
  public static void dropAll() {
    Cassandra cassandra = new Cassandra(addrs, port);

    Drop dropKs = SchemaBuilder.dropKeyspace(KEYSPACE).ifExists();
    cassandra.directExecute(dropKs.build());

    cassandra.close();
  }
  */

  @Test
  public void simpleQuery() {
    Cassandra cassandra = new Cassandra(addrs, port);
    ResultSet rs = cassandra.directExecute("select release_version from system.local");
    Row actual = rs.one();
    assertThat(actual.getString("release_version")).isEqualTo(CASSANDRA_VERSION);
    cassandra.close();
  }

  @Test
  public void get() {
    Cassandra cassandra = new Cassandra(addrs, port);
    List<Key> pk = new ArrayList<Key>(Arrays.asList(new Key("key", "local")));
    Get get = new Get("system", "local", pk);
    ResultSet rs = cassandra.get(get);
    Row actual = rs.one();
    assertThat(actual.getString("release_version")).isEqualTo(CASSANDRA_VERSION);
    cassandra.close();
  }

  @Test
  public void put() {
    Cassandra cassandra = new Cassandra(addrs, port);
    List<Key> pk = new ArrayList<Key>(Arrays.asList(new Key(PK, "forPut")));
    Put put = new Put(KEYSPACE, TABLE, pk);
    put.withValue(C1, 100).withValue(C2, 200);
    cassandra.put(put);

    Put put2 = new Put(KEYSPACE, TABLE, pk);
    put2.withValue(C1, 111).withValue(C2, 222);
    // not applied
    cassandra.put(put2);

    Get get = new Get(KEYSPACE, TABLE, pk);
    ResultSet rs = cassandra.get(get);
    Row actual = rs.one();
    assertThat(actual.getInt(C1)).isEqualTo(100);
    assertThat(actual.getInt(C2)).isEqualTo(200);

    cassandra.close();
  }

  @Test
  public void update() {
    Cassandra cassandra = new Cassandra(addrs, port);
    List<Key> pk = new ArrayList<Key>(Arrays.asList(new Key(PK, "forUpdate")));
    Put put = new Put(KEYSPACE, TABLE, pk);
    put.withValue(C1, 100).withValue(C2, 200);
    cassandra.put(put);

    Update update = new Update(KEYSPACE, TABLE, pk);
    update.withValue(C1, 111).withValue(C2, 222);
    cassandra.updates(update);

    Get get = new Get(KEYSPACE, TABLE, pk);
    ResultSet rs = cassandra.get(get);
    Row actual = rs.one();
    assertThat(actual.getInt(C1)).isEqualTo(111);
    assertThat(actual.getInt(C2)).isEqualTo(222);

    cassandra.close();
  }

  @Test
  public void updateWithCondition() {
    Cassandra cassandra = new Cassandra(addrs, port);
    List<Key> pk = new ArrayList<Key>(Arrays.asList(new Key(PK, "forUpdateWithCondition")));
    Put put = new Put(KEYSPACE, TABLE, pk);
    put.withValue(C1, 100).withValue(C2, 200);
    cassandra.put(put);

    Update update1 = new Update(KEYSPACE, TABLE, pk);
    update1.withValue(C1, 111).withCondition(C1, 101).withCondition(C2, 200);
    // not applied
    cassandra.updates(update1);

    Update update2 = new Update(KEYSPACE, TABLE, pk);
    update2.withValue(C1, 1111).withCondition(C1, 100).withCondition(C2, 200);
    // will be applied
    cassandra.updates(update2);

    Get get = new Get(KEYSPACE, TABLE, pk);
    ResultSet rs = cassandra.get(get);
    Row actual = rs.one();
    assertThat(actual.getInt(C1)).isEqualTo(1111);
    assertThat(actual.getInt(C2)).isEqualTo(200);

    cassandra.close();
  }
}
