package us.yuxin.hump;

import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;

public class HumpCollector implements Runnable {
  Configuration conf;
  BlockingQueue<String> feedbackQueue;

  public void setup(Configuration conf, BlockingQueue<String> feedbackQueue) {
    this.conf = conf;
    this.feedbackQueue = feedbackQueue;
  }

  public void run() {
    System.out.println("COLLECTION-BEGIN");
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

      System.out.println(res);
    }
    System.out.println("COLLECTION-END");
  }
}
