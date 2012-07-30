package us.yuxin.hump;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.client.HazelcastClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HumpTaskRecordReader extends RecordReader<Text, Text> {
  BlockingQueue<String> jobQueue;
  TaskAttemptContext context;
  int serial;

  private Text curKey;
  private Text curValue;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    serial = 0;
    Configuration conf = context.getConfiguration();

    HazelcastClient client = GridClient.getClient(conf);
    jobQueue = client.getQueue(Hump.HUMP_HAZELCAST_TASK_QUEUE);

    this.context = context;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    String job = jobQueue.take();
    if (job.equals("STOP"))
      return false;

    curKey = new Text(Integer.toString(serial));
    curValue = new Text(job);
    serial += 1;

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
    GridClient.shutdown();
  }
}
