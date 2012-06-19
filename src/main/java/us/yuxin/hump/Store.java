package us.yuxin.hump;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;

public interface Store {
  void setUseTemporary(boolean useTemporary);
  void store(Path file, JdbcSource source, Properties prop) throws IOException;
  void store(Path file, JdbcSource source, Properties prop, StoreCounter counter) throws IOException;
  Path getLastRealPath();
  Path getLastTempPath();

  String getFormatId();
}
