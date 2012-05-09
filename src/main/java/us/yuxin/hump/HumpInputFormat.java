package us.yuxin.hump;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HumpInputFormat extends InputFormat<Text, Text> {
  private final static int DEFAULT_TASK_PARALLEL = 20;

  /**
   * Parallel
   */
  private int parallel;


  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    parallel = context.getConfiguration().getInt(Hump.CONF_HUMP_TASKS, DEFAULT_TASK_PARALLEL);

    ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < parallel; ++i) {
      EmptySplit split = new EmptySplit();
      splits.add(split);
    }
    return splits;
  }

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    return new HumpTaskRecordReader();
  }

  public static class EmptySplit extends InputSplit implements Writable {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }
}
