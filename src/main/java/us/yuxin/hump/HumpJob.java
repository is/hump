package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class HumpJob {
  public static final String CONF_HUMP_TASKS = "hump.tasks";

  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = new Configuration();

    DistributedCache.addArchiveToClassPath(new Path("/is/app/hump/lib/mysql-connector-java-5.1.19.jar"), conf);

    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
    conf.setInt(CONF_HUMP_TASKS, 22);

    Job job = new Job(conf);

    job.setJobName("Hump-Sample");
    job.setJarByClass(HumpMapper.class);
    job.setMapperClass(HumpMapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setInputFormatClass(HumpInputFormat.class);
    FileInputFormat.addInputPath(job, new Path("ignored"));

    job.setNumReduceTasks(0);
    job.waitForCompletion(true);
  }
}
