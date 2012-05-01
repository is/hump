package us.yuxin.hump;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HumpMapper extends Mapper<Text, Text, Text, NullWritable> {
  private static Log log = LogFactory.getLog("org.apache.hadoop.mapred.Task");

  @Override
  protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    System.out.println("key:" + key.toString() + ", value:" + value.toString());
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    System.out.println("HumpMapper.setup");
    log.info("HumpMapper.setup");
  }

  public HumpMapper() {
    super();
    System.out.println("HumpMapper.HumpMapper");
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    log.info("HumpMapper.run");
    super.run(context);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    System.out.println("clean up Hello World - tid:" + context.getTaskAttemptID());
  }
}
