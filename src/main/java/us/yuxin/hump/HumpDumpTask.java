package us.yuxin.hump;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HumpDumpTask implements HumpTask {
  @Override
  public void setup(Mapper.Context context) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void run(Mapper.Context context, Text serial, Text taskInfo) {
    System.out.println("HumpDumpTask.run -- " + serial.toString() + ":" + taskInfo.toString());
  }

  @Override
  public void cleanup(Mapper.Context context) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
