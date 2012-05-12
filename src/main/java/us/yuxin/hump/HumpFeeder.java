package us.yuxin.hump;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class HumpFeeder implements Runnable {
  private int parallel;

  private BlockingQueue<String> taskQueue;
  private BlockingQueue<String> feedbackQueue;
  private Configuration conf;
  List<String> tasks;

  public void setup(Configuration conf, File[] sources,
    BlockingQueue<String> taskQueue,
    BlockingQueue<String> feedbackQueue, int parallel) {
    this.conf = conf;

    this.taskQueue = taskQueue;
    this.feedbackQueue = feedbackQueue;
    this.parallel = parallel;

    setupSources(sources);
  }

  private void setupSources(File[] sources) {
    ObjectMapper mapper = new ObjectMapper();
    tasks = new LinkedList<String>();

    for (File source : sources) {
      try {
        JsonNode root = mapper.readValue(source, JsonNode.class);
        for (JsonNode node : root) {
          tasks.add(mapper.writeValueAsString(node));
        }
      } catch (IOException e) {
        e.printStackTrace();
        // TODO Exception handler.
      }
    }

    if (conf.getBoolean(Hump.CONF_HUMP_TASK_SHUFFLE, true)) {
      Collections.shuffle(tasks);
    }
  }


  public void run() {
    String feedCmd = "{\"type\":\"feed.notify.tasks\", \"tasks\":" + tasks.size() + "}";
    feedbackQueue.offer(feedCmd);

    for (String task : tasks) {
      taskQueue.offer(task);
    }
    for (int i = 0; i < parallel; ++i) {
      taskQueue.offer("STOP");
    }
  }
}
