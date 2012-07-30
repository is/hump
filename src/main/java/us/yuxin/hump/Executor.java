package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public interface Executor {
  public void setup(Mapper.Context context) throws IOException, InterruptedException;

  public void run(Mapper.Context context, Text serial, Text taskInfo) throws IOException, InterruptedException;

  public void cleanup(Mapper.Context context) throws IOException, InterruptedException;
}
