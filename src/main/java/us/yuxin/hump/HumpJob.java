package us.yuxin.hump;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Member;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class HumpJob {
  public static final String CONF_HUMP_TASKS = "hump.tasks";
  public static final String CONF_HUMP_HAZELCAST_ENDPOINT = "hump.hazelcast.endpoint";

  public static final String HUMP_QUEUE_TASK = "hump.queue.task";
  // public static final String HUMP_QUEUE_RESULT = "hump.queue.result";

  public static int parallelTasks = 22;


  public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

    Configuration conf = new Configuration();

    DistributedCache.addArchiveToClassPath(new Path("/is/app/hump/lib/hazelcast-2.0.3.jar"), conf);
    DistributedCache.addArchiveToClassPath(new Path("/is/app/hump/lib/mysql-connector-java-5.1.19.jar"), conf);
    DistributedCache.addArchiveToClassPath(new Path("/is/app/hump/lib/hazelcast-client-2.0.3.jar"), conf);

    Config hcfg = new Config();
    hcfg.setGroupConfig(new GroupConfig("hump", "humps"));
    Hazelcast.init(hcfg);

    Member member = Hazelcast.getCluster().getLocalMember();
    InetSocketAddress addr = member.getInetSocketAddress();
    BlockingQueue<String> taskQueue = Hazelcast.getQueue(HUMP_QUEUE_TASK);

    for (int i = 0; i < parallelTasks * 5; ++i) {
      taskQueue.put(Integer.toString(i));
    }

    for (int i = 0; i < parallelTasks; ++i) {
      taskQueue.put("STOP");
    }

    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    conf.setInt(CONF_HUMP_TASKS, parallelTasks);
    conf.set(CONF_HUMP_HAZELCAST_ENDPOINT, addr.getAddress().getHostAddress() + ":" + addr.getPort());

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

    Hazelcast.shutdownAll();
  }
}
