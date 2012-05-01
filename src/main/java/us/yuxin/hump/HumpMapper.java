package us.yuxin.hump;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HumpMapper extends Mapper<Text, Text, Text, NullWritable> {
  private static Log log = LogFactory.getLog(HumpMapper.class);
  private HumpTask task;

  @Override
  protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    System.out.println("key:" + key.toString() + ", value:" + value.toString());
    Thread.sleep(1000);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    super.setup(context);
    try {
      task = (HumpTask) Class.forName(conf.get(Hump.CONF_HUMP_TASK_CLASS)).newInstance();
      task.setup(context);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new IOException("Can't load jdbc class", e);
    } catch (InstantiationException e) {
      e.printStackTrace();
      throw new IOException("Can't load hump task class", e);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      throw new IOException("Can't load hump task class", e);
    }
  }

  public HumpMapper() {
    super();
    System.out.println("HumpMapper.HumpMapper");
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    super.run(context);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    task.cleanup(context);
    System.out.println("clean up Hello World - tid:" + context.getTaskAttemptID());
  }
}
