package us.yuxin.hump;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.client.HazelcastClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;


public class HumpDumpTask implements HumpTask {
  FileSystem fs;
  RCFileStore store;
  Configuration conf;

  BlockingQueue<String> feedbackQueue;
  HazelcastClient client;

  StoreCounter globalCounter;
  StoreCounter singleCounter;
  int taskCounter;

  @Override
  public void setup(Mapper.Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
    client = HumpGridClient.getClient(conf);
    feedbackQueue = client.getQueue(Hump.HUMP_HAZELCAST_FEEDBACK_QUEUE);

    fs = FileSystem.get(context.getConfiguration());
    CompressionCodec codec = null;

    globalCounter = new StoreCounter();
    singleCounter = new StoreCounter();
    taskCounter = 0;

    if (conf.get(Hump.CONF_HUMP_COMPRESSION_CODEC) != null) {
      try {
        codec = (CompressionCodec) conf.getClass(Hump.CONF_HUMP_COMPRESSION_CODEC, null).newInstance();
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
    singleCounter.reset();
    ++taskCounter;
    long beginTime;
    long endTime;

    beginTime = System.currentTimeMillis();

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
    try {
      source.open();
      store.store(new Path(target), source, null, singleCounter);
      source.close();
      endTime = System.currentTimeMillis();
      singleCounter.during = endTime - beginTime;
    } catch (SQLException e) {
      e.printStackTrace();
      // TODO Exception handler
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      // TODO Exception handler
    }

    globalCounter.plus(singleCounter);
    System.out.println("OK...");

    // ---- Send feedback.
    ObjectNode feedback = mapper.createObjectNode();

    String id;
    if (root.get("id") != null) {
      id = root.get("id").getTextValue();
    } else if (root.get("table") != null) {
      id = root.get("table").getTextValue();
    } else {
      id = context.getTaskAttemptID().toString() + "/" + taskCounter;
    }

    feedback.put("id", id);
    feedback.put("target", root.get("target").getTextValue());
    feedback.put("status", "OK");
    feedback.put("code", 0);
    feedback.put("rows", singleCounter.rows);
    feedback.put("cells", singleCounter.cells);
    feedback.put("nullCells", singleCounter.nullCells);
    feedback.put("cellBytes", singleCounter.bytes);
    feedback.put("beginTime", new SimpleDateFormat("yyyyMMdd.kkmmss").format(new Date(beginTime)));
    feedback.put("during", singleCounter.during);
    feedback.put("taskid", context.getTaskAttemptID().toString());

    feedbackQueue.offer(mapper.writeValueAsString(feedback));
    // ObjectMapper mapper = new ObjectMapper();
    // JsonNode rootNode = mapper.readValue(taskInfo.toString(), JsonNode.class);
    // System.out.println("ID:" + rootNode.get("id").getIntValue());
    // Thread.sleep(1500);
  }

  @Override
  public void cleanup(Mapper.Context context) throws IOException, InterruptedException {
    fs.close();
  }
}
