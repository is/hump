package us.yuxin.hump;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.GroupConfig;
import org.apache.hadoop.conf.Configuration;

public class GridClient {
  static HazelcastClient client;

  public static synchronized  HazelcastClient getClient(Configuration conf) {
    if (client == null) {
      ClientConfig cfg = new ClientConfig();
      cfg.setGroupConfig(new GroupConfig(conf.get(Hump.CONF_HUMP_HAZELCAST_GROUP), conf.get(Hump.CONF_HUMP_HAZELCAST_PASSWORD)));
      cfg.addAddress(conf.get(Hump.CONF_HUMP_HAZELCAST_ENDPOINT));

      client = HazelcastClient.newHazelcastClient(cfg);
    }
    return client;
  }

  public static synchronized HazelcastClient getClient() {
    return client;
  }

  public static synchronized void shutdown() {
    if (client != null) {
      client.shutdown();
      client = null;
    }
  }
}
