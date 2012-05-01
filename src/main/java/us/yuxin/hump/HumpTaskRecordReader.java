package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Created with IntelliJ IDEA.
 * User: is
 * Date: 5/1/12
 * Time: 12:35 AM
 * To change this template use File | Settings | File Templates.
 */
public class HumpTaskRecordReader extends RecordReader<Text, Text> {
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    System.out.println("HumpTaskRecordReader.initialize");
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void close() throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
