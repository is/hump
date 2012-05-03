package us.yuxin.hump;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;


public class HumpDumpTask implements HumpTask {
  FileSystem fs;
  RCFileStore store;
  Configuration conf;

  @Override
  public void setup(Mapper.Context context)  throws IOException, InterruptedException {
    conf = context.getConfiguration();
    fs = FileSystem.get(context.getConfiguration());
    CompressionCodec codec = null;

    if (conf.get(Hump.CONF_HUMP_COMPRESSION_CODEC) != null) {
      try {
        codec = (CompressionCodec)conf.getClass(Hump.CONF_HUMP_COMPRESSION_CODEC, null).newInstance();
      } catch (InstantiationException e) {
        e.printStackTrace();
        throw new IOException("Invalid compression codec", e);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        throw new IOException("Invalid compression codec", e);
      }
    }
    store = new RCFileStore(fs, conf, codec);
  }

  @Override
  public void run(Mapper.Context context, Text serial, Text taskInfo) throws IOException, InterruptedException {
    System.out.println("HumpDumpTask.run -- " + serial.toString() + ":" + taskInfo.toString());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readValue(taskInfo.toString(), JsonNode.class);

    JdbcSource source = new JdbcSource();

    source.setDriver(root.get("driver").getTextValue());
    source.setUrl(root.get("url").getTextValue());
    source.setUsername(root.get("username").getTextValue());
    if (root.get("password") != null)
      source.setPassword(root.get("password").getTextValue());
    if (root.get("query") != null)
      source.setQuery(root.get("query").getTextValue());
    if (root.get("table") != null) {
      String stmt = "SELECT * FROM " + root.get("table").getTextValue();
      source.setQuery(stmt);
    }

    String target = root.get("target").getTextValue();
    store.store(new Path(target), source, null);
    try {
      source.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    System.out.println("OK...");
    // ObjectMapper mapper = new ObjectMapper();
    // JsonNode rootNode = mapper.readValue(taskInfo.toString(), JsonNode.class);
    // System.out.println("ID:" + rootNode.get("id").getIntValue());
    // Thread.sleep(1500);
  }

  @Override
  public void cleanup(Mapper.Context context) throws IOException, InterruptedException  {
    fs.close();
  }
}
