package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HumpDumpTask implements HumpTask {
  @Override
  public void setup(Mapper.Context context)  throws IOException, InterruptedException {
  }

  @Override
  public void run(Mapper.Context context, Text serial, Text taskInfo) throws IOException, InterruptedException {
    System.out.println("HumpDumpTask.run -- " + serial.toString() + ":" + taskInfo.toString());
    Thread.sleep(1500);
  }

  @Override
  public void cleanup(Mapper.Context context) throws IOException, InterruptedException  {
  }
}
