package us.yuxin.hump;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

public class EmptySplit extends InputSplit {
  public void write(DataOutput out) throws IOException { }
  public void readFields(DataInput in) throws IOException { }
  public long getLength() { return 0L; }
  public String[] getLocations() { return new String[0]; }
}
