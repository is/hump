package us.yuxin.hump;

import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;

public class HumpCollector implements Runnable {
  Configuration conf;
  int taskCounter;
  BlockingQueue<String> feedbackQueue;

  public void setup(Configuration conf, BlockingQueue<String> feedbackQueue) {
    this.conf = conf;
    this.feedbackQueue = feedbackQueue;
    this.taskCounter = 0;
  }

  public void run() {
    System.out.println("COLLECTOR-BEGIN");
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

      System.out.format("%05d -- %s\n", taskCounter, res);
      ++taskCounter;
    }
    System.out.println("COLLECTOR-END " + taskCounter + " tasks");
  }
}
