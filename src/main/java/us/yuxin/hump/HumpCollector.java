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
 * hump-log-full.json
 * hump-log-summary.json
 * hump-log-failure.json
 */
public class HumpCollector implements Runnable {
  Configuration conf;

  int taskCounter;
  int successCounter;
  int failureCounter;

  int feederTasks;

  BlockingQueue<String> feedbackQueue;
  Log log = LogFactory.getLog(HumpCollector.class);

  FileWriter fullLog, summaryLog, failureLog;

  public static class ArrayCounter {
    int counter[];
    int size;
    int used;
    int point;

    public ArrayCounter(int size) {
      this.size = size;
      this.used = 0;
      this.point = 0;
      this.counter = new int[size];
    }

    public void push(int v) {
      counter[point] = v;
      if (used != size) {
        ++used;
      }
      ++point;
      if (point == size)
        point = 0;
    }

    public float average() {
      int sum = 0;
      for (int i = 0; i < used; ++i) {
        sum += counter[i];
      }
      return sum * 1f / used;
    }
  }

  public void setup(Configuration conf, BlockingQueue<String> feedbackQueue) {
    this.conf = conf;
    this.feedbackQueue = feedbackQueue;

    this.taskCounter = 0;
    this.successCounter = 0;
    this.failureCounter = 0;

    this.failureLog = null;
    this.summaryLog = null;
    this.fullLog = null;
  }


  public void run() {
    openLogFiles();
    ObjectMapper om  = new ObjectMapper();

    long totalInBytes = 0;
    long totalOutBytes = 0;
    // long totalDuring = 0;
    long totalRows = 0;
    long beginTS = System.currentTimeMillis();

    long curSecond = 0;
    int secondCounter = 0;
    ArrayCounter tableCounter = new ArrayCounter(10);
    float tableSpeed = 0f;

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

      LogFileUtils.writeln(fullLog, res);

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

        if (retCode != Hump.RETCODE_ERROR) {
          long lastSecond = System.currentTimeMillis() / 1000;
          if (lastSecond != curSecond) {
            curSecond = lastSecond;
            tableCounter.push(secondCounter);
            tableSpeed = tableCounter.average();
            secondCounter = 0;
          }
          secondCounter += 1;
        }

        if (retCode == Hump.RETCODE_SKIP) {
          successCounter += 1;

          String msg = String.format("%d/%d {%s} skipped, %s",
            taskCounter, feederTasks, id, root.get("message").getTextValue());
          log.info(msg);
          LogFileUtils.writelnWithTS(summaryLog, msg);
        } else if (retCode == Hump.RETCODE_OK) {
          successCounter += 1;

          long rows = root.get("rows").getLongValue();
          long inBytes = root.get("cellBytes").getLongValue();
          long during = root.get("during").getLongValue();
          long outBytes = root.get("fileBytes").getLongValue();

          totalRows += rows;
          totalInBytes += inBytes;
          totalOutBytes += outBytes;
          // totalDuring += during;

          String msg = String.format("%d/%d {%s} rows:%,d, inbytes:%,d, outbytes:%,d, during:%.3fs [%.1f]",
            taskCounter, feederTasks, id, rows, inBytes, outBytes, during * 0.001f, tableSpeed);
          log.info(msg);
          LogFileUtils.writelnWithTS(summaryLog, msg);
        } else { // RETCODE_ERROR
          failureCounter += 1;
          String msg = String.format("%d/%d {%s} failed, msg: %s",
            taskCounter, feederTasks, id, root.get("message").getTextValue());
          log.info(msg);

          LogFileUtils.writelnWithTS(summaryLog, msg);
          LogFileUtils.writelnWithTS(failureLog, msg);
        }
      } catch (IOException e) {
        e.printStackTrace();
        // TODO Exception handler.
      }
    }

    log.info(
      String.format("-- Statistic -- use %.2fs, %d tables (%d failed/%.3f%%), total row:%,d inbytes:%,d, outbytes:%,d",
      (System.currentTimeMillis() - beginTS) * 0.001f, taskCounter, failureCounter,
      failureCounter * 100f / taskCounter, totalRows, totalInBytes, totalOutBytes));

    closeLogFiles();
  }


  private void openLogFiles() {
    fullLog = LogFileUtils.open(conf.get(Hump.CONF_HUMP_RESULT_FULL, "hump-log-full"));
    summaryLog = LogFileUtils.open(conf.get(Hump.CONF_HUMP_RESULT_SUMMARY, "hump-log-summary"));
    failureLog = LogFileUtils.open(conf.get(Hump.CONF_HUMP_RESULT_FAILURE, "hump-log-failure"));
  }

  private void closeLogFiles() {
    LogFileUtils.close(fullLog);
    LogFileUtils.close(summaryLog);
    LogFileUtils.close(failureLog);

    fullLog = null;
    summaryLog = null;
    failureLog = null;
  }
}
