package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HumpTaskRecordReader extends RecordReader<Text, Text> {
  private boolean first = true;
  private Text key = new Text("key");
  private Text value = new Text("value");
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    System.out.println("HumpTaskRecordReader.initialize");
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    System.out.println("HumpTaskRecordReader.nexKeyValue:" + first);
    if (first) {
      first = false;
      return true;
    }
    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    System.out.println("HumpTaskRecordReader.close");
  }
}
