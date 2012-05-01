package us.yuxin.hump;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;


public class HumpDumpTask implements HumpTask {
  FileSystem fs;
  RCFileStore store;

  @Override
  public void setup(Mapper.Context context)  throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    fs = FileSystem.get(context.getConfiguration());
    CompressionCodec codec = null;

    if (conf.get(Hump.CONF_HUMP_COMPRESSION_CODEC) != null) {
      try {
        codec = (CompressionCodec)conf.getClass(Hump.CONF_HUMP_COMPRESSION_CODEC, null).newInstance();
      } catch (InstantiationException e) {
        e.printStackTrace();
        throw new IOException("Invalid compression codec", e);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
        throw new IOException("Invalid compression codec", e);
      }
    }
    store = new RCFileStore(fs, conf, codec);
  }

  @Override
  public void run(Mapper.Context context, Text serial, Text taskInfo) throws IOException, InterruptedException {
    System.out.println("HumpDumpTask.run -- " + serial.toString() + ":" + taskInfo.toString());

    JSONObject obj;
    try {
      obj = new JSONObject(taskInfo.toString());
      System.out.println(obj.getInt("id"));

    } catch (JSONException e) {
      e.printStackTrace();
      throw new IOException("Invalid json task info", e);
    }
    Thread.sleep(1500);
  }

  @Override
  public void cleanup(Mapper.Context context) throws IOException, InterruptedException  {
    fs.close();
  }
}
