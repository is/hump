package us.yuxin.hump;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

public class TextStore extends StoreBase {
  int rowDelimiter = 1;
  int fieldDelimiter = '\n';

  public TextStore(FileSystem fs, Configuration conf, CompressionCodec codec) {
    super(fs, conf, codec);
    // TODO row/field delimiter can be set.
  }

  @Override
  public void store(Path file, JdbcSource source, Properties prop, StoreCounter counter) throws IOException {
    if (source == null || !source.isReady()) {
      throw new IOException("JDBC Source is not ready");
    }
    setLastRealPath(file);

    if (useTemporary) {
      file = genTempPath();
    }

    if (counter == null) {
      counter = new StoreCounter();
    }

    JdbcSourceMetadata jdbcMetadata = new JdbcSourceMetadata();
    try {
      jdbcMetadata.setJdbcSource(source);
    } catch (SQLException e) {
      throw new IOException("Can't generate metadata from JDBC source", e);
    }

    int columns = jdbcMetadata.getColumnCount();
    ResultSet rs = source.getResultSet();

    OutputStream outs = fs.create(file);
    if (codec != null) {
      outs = codec.createOutputStream(outs);
    }


    try {
      while (rs.next()) {
        ++counter.rows;
        for (int c = 1; c <= columns; ++c) {
          Object value;
          try {
            value = rs.getObject(c);
            ++counter.cells;
          } catch (SQLException e) {
            throw new IOException("Failed to fetch data from JDBC source, column is " + jdbcMetadata.names[c] + "/" + c, e);
          }

          if (value == null) {
            ++counter.nullCells;
          } else {
            byte[] bytes = value.toString().getBytes("UTF-8");
            counter.inBytes += bytes.length;
            outs.write(bytes);
          }

          if (c == columns)
            outs.write(rowDelimiter);
          else
            outs.write(fieldDelimiter);
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to fetch data from JDBC source", e);
    }
    outs.close();
    counter.outBytes = fs.getFileStatus(file).getLen();

    if (useTemporary) {
      fs.mkdirs(getLastRealPath().getParent());
      fs.rename(getLastTempPath(), getLastRealPath());
    }
  }


  @Override
  public String getFormatId() {
    return "text";
  }
}
