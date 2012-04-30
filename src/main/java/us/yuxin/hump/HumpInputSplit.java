package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;


public class HumpInputSplit extends InputSplit {
  int taskId;

  public HumpInputSplit(int taskId) {
    this.taskId = taskId;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }

  public int getTaskId() {
    return taskId;
  }
}
