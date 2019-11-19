package cdriver4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ClusterManager {
  private CqlSessionBuilder builder = null;
  private CqlSession session = null;

  public ClusterManager() {}

  public synchronized CqlSession getSession(List<String> addrs, int port) {
    List<InetSocketAddress> contactPoints = new ArrayList<InetSocketAddress>();

    if (session == null) {
      try {
        if (builder == null) {
          addrs.forEach(addr -> contactPoints.add(new InetSocketAddress(addr, port)));

          // TODO: LoadBalancePolicy and RetryPolicy
          builder =
              CqlSession.builder()
                  .addContactPoints(contactPoints)
                  .withLocalDatacenter("datacenter1");  // for SimpleSnitch
        }
        session = builder.build();
      } catch (RuntimeException e) {
        System.err.println("Failed to build a session: " + e.getMessage());
        throw e;
      }
    }

    return session;
  }

  public void close() {
    session.close();
    session = null;
  }
}
