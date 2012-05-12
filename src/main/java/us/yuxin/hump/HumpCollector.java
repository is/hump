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

          continue;

        }


        ++taskCounter;

        String id = node.get("id").getTextValue();
        int retCode = node.get("code").getIntValue();
        long rows = node.get("rows").getLongValue();
        long bytes = node.get("bytes").getLongValue();
        long during = node.get("during").getLongValue();

        if (retCode == 0) {
          logger.info("" + taskCounter + "/" + feederTasks + " -- " +
            id + ": " + rows + " rows/" + bytes + " bytes, " +
            "during: " + during * 0.001f + "s");
        }

        // System.out.format("%05d -- %s\n", taskCounter, res);

      } catch (IOException e) {
        e.printStackTrace();
        // TODO Exception handler.
      }

    }
  }
}
