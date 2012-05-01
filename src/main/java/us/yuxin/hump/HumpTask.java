package us.yuxin.hump;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public interface HumpTask {
  public void setup(Mapper.Context context);
  public void run(Mapper.Context context, Text serial, Text taskInfo);
  public void cleanup(Mapper.Context context);
}
