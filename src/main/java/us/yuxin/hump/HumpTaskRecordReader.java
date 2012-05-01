package us.yuxin.hump;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.GroupConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HumpTaskRecordReader extends RecordReader<Text, Text> {
  HazelcastClient client;
  BlockingQueue<String> jobQueue;
  TaskAttemptContext context;

  private Text curKey;
  private Text curValue;
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    System.out.println("HumpTaskRecordReader.initialize");
    Configuration conf = context.getConfiguration();

    ClientConfig cfg = new ClientConfig();
    cfg.setGroupConfig(new GroupConfig("hump", "humps"));
    cfg.addAddress(conf.get(HumpJob.CONF_HUMP_HAZELCAST_ENDPOINT));

    client = HazelcastClient.newHazelcastClient(cfg);
    jobQueue = client.getQueue(HumpJob.HUMP_QUEUE_TASK);

    this.context = context;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    String job = jobQueue.take();
    if (job.equals("STOP"))
      return false;

    curKey = new Text(job);
    curValue = curKey;
    return true;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return curKey;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return curValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    System.out.println("HumpTaskRecordReader.close");
    client.shutdown();
  }
}
