package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HumpMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
  @Override
  public void run(Context context) throws IOException, InterruptedException {
    System.out.println(context.getTaskAttemptID());
  }
}
