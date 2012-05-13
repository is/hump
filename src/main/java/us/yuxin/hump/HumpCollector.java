package us.yuxin.hump;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;


/**
 *
 * Output:
 * hump-res-full.json
 * hump-res-summary.json
 * hump-res-success.json
 * hump-res-failure.json
 */
public class HumpCollector implements Runnable {
  Configuration conf;

  int taskCounter;
  int successCounter;
  int failureCounter;

  int feederTasks;

  BlockingQueue<String> feedbackQueue;
  Log log = LogFactory.getLog(HumpCollector.class);

  FileWriter resFull, resSummary, resFailure;

  public void setup(Configuration conf, BlockingQueue<String> feedbackQueue) {
    this.conf = conf;
    this.feedbackQueue = feedbackQueue;

    this.taskCounter = 0;
    this.successCounter = 0;
    this.failureCounter = 0;

    this.resFailure = null;
    this.resSummary = null;
    this.resFull = null;
  }


  public void run() {
    openLogFiles();
    ObjectMapper om  = new ObjectMapper();

    long totalBytes = 0;
    long totalDuring = 0;
    long totalRows = 0;

    long beginTS = System.currentTimeMillis();

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

      LogFileUtils.writeln(resFull, res);

      try {
        JsonNode root = om.readValue(res, JsonNode.class);

        if (root.get("type") != null) {
          String msgType = root.get("type").getTextValue();

          if (msgType.equals("feed.notify.tasks")) {
            feederTasks = root.get("tasks").getIntValue();
            log.info("Feed notification: " + feederTasks + " tasks");
          }
          continue;
        }

        ++taskCounter;

        String id = root.get("id").getTextValue();
        int retCode = root.get("code").getIntValue();

        if (retCode == 0) {
          successCounter += 1;

          long rows = root.get("rows").getLongValue();
          long bytes = root.get("cellBytes").getLongValue();
          long during = root.get("during").getLongValue();

          totalRows += rows;
          totalBytes += bytes;
          totalDuring += during;

          String msg = String.format("%d/%d {%s} rows:%,d, inbytes:%,d, during:%.3fs",
            taskCounter, feederTasks, id, rows, bytes, during * 0.001f);
          log.info(msg);
          LogFileUtils.writelnWithTS(resSummary, msg);
        } else {
          failureCounter += 1;
          String msg = String.format("%d/%d {%s} failed, msg: %s",
            taskCounter, feederTasks, id, root.get("message").getTextValue());
          log.info(msg);

          LogFileUtils.writelnWithTS(resSummary, msg);
          LogFileUtils.writelnWithTS(resFailure, msg);
        }
      } catch (IOException e) {
        e.printStackTrace();
        // TODO Exception handler.
      }
    }

    log.info(String.format("-- Statistic -- use %.2fs, %d tables (%d failed/%.3f%%), total row:%,d inbytes:%,d",
      (System.currentTimeMillis() - beginTS) * 0.001f, taskCounter, failureCounter,
      failureCounter * 100f / taskCounter, totalRows, totalBytes));

    closeLogFiles();
  }


  private void openLogFiles() {
    resFull = LogFileUtils.open(conf.get(Hump.CONF_HUMP_RESULT_FULL, "hump-res-full"));
    resSummary = LogFileUtils.open(conf.get(Hump.CONF_HUMP_RESULT_SUMMARY, "hump-res-summary"));
    resFailure = LogFileUtils.open(conf.get(Hump.CONF_HUMP_RESULT_FAILURE, "hump-res-failure"));
  }

  private void closeLogFiles() {
    LogFileUtils.close(resFull);
    LogFileUtils.close(resSummary);
    LogFileUtils.close(resFailure);

    resFull = null;
    resSummary = null;
    resFailure = null;
  }
}
