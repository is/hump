package us.yuxin.hump;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class HumpCollector implements Runnable {
  Configuration conf;

  int taskCounter;
  int feederTasks;

  BlockingQueue<String> feedbackQueue;

  Log logger = LogFactory.getLog(HumpCollector.class);

  public void setup(Configuration conf, BlockingQueue<String> feedbackQueue) {
    this.conf = conf;
    this.feedbackQueue = feedbackQueue;
    this.taskCounter = 0;
  }

  public void run() {
    ObjectMapper om  = new ObjectMapper();

    while (true) {
      String res;
      try {
        res = feedbackQueue.take();

      } catch (InterruptedException e) {
        e.printStackTrace();
        break;
      }

      if (res.equals("STOP"))
        break;

      try {
        JsonNode node = om.readValue(res, JsonNode.class);

        if (node.get("type") != null) {
          String msgType = node.get("type").getTextValue();

          if (msgType.equals("feed.notify.tasks")) {
            feederTasks = node.get("tasks").getIntValue();
            logger.info("Feed notification: " + feederTasks + " tasks");
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        // TODO Exception handler.
      }
      ++taskCounter;
      System.out.format("%05d -- %s\n", taskCounter, res);
    }
  }
}
