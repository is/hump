package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HumpMapper extends Mapper<Text, Text, Text, NullWritable> {

  @Override
  protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    System.out.println("key:" + key.toString() + ", value:" + value.toString());
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    System.out.println("clean up Hello World - tid:" + context.getTaskAttemptID());
  }
}
