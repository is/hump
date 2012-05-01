package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class HumpJob {
  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
    Job job = new Job(conf);

    job.setJobName("Hump-Sample");

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setInputFormatClass(HumpInputFormat.class);
    FileInputFormat.addInputPath(job, new Path("ignored"));

    job.setJarByClass(HumpMapper.class);
    job.waitForCompletion(true);
  }
}
