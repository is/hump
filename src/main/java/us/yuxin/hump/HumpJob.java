package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class HumpJob {
  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();
    Job job = new Job(conf);

    job.setJobName("Hump-Sample");

    job.setInputFormatClass(HumpInputFormat.class);
    job.setJarByClass(HumpMapper.class);
    job.waitForCompletion(true);
  }
}
