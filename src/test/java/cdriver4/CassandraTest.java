package cdriver4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class CassandraTest {
  private final List<String> addrs = new ArrayList<String>(Arrays.asList("localhost"));
  private final int port = 9042;
  private final String CASSANDRA_VERSION = "3.11.4-SNAPSHOT";

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
    List<Key> pk = new ArrayList<Key>(Arrays.asList(new Key("id", "forPut")));
    Put put = new Put("testks", "testtbl", pk);
    put.withValue("val1", 100).withValue("val2", 200);
    cassandra.put(put);

    Put put2 = new Put("testks", "testtbl", pk);
    put2.withValue("val1", 111).withValue("val2", 222);
    // not applied
    cassandra.put(put2);

    Get get = new Get("testks", "testtbl", pk);
    ResultSet rs = cassandra.get(get);
    Row actual = rs.one();
    assertThat(actual.getInt("val1")).isEqualTo(100);
    assertThat(actual.getInt("val2")).isEqualTo(200);

    cassandra.close();
  }

  @Test
  public void update() {
    Cassandra cassandra = new Cassandra(addrs, port);
    List<Key> pk = new ArrayList<Key>(Arrays.asList(new Key("id", "forUpdate")));
    Put put = new Put("testks", "testtbl", pk);
    put.withValue("val1", 100).withValue("val2", 200);
    cassandra.put(put);

    Update update = new Update("testks", "testtbl", pk);
    update.withValue("val1", 111).withValue("val2", 222);
    // not applied
    cassandra.updates(update);

    Get get = new Get("testks", "testtbl", pk);
    ResultSet rs = cassandra.get(get);
    Row actual = rs.one();
    assertThat(actual.getInt("val1")).isEqualTo(111);
    assertThat(actual.getInt("val2")).isEqualTo(222);

    cassandra.close();

  }
}
