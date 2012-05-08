package us.yuxin.hump;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;


public class RCFileStore implements Store {
  public final String NULL_STRING = "NULL";

  FileSystem fs;
  Configuration conf;
  CompressionCodec codec;
  StoreCounter counter;

  public RCFileStore(FileSystem fs, Configuration conf, CompressionCodec codec) {
    this.fs = fs;
    this.conf = conf;
    this.codec = codec;
    this.counter = new StoreCounter();
  }


  public void store(Path file, JdbcSource source, Properties prop) throws IOException {
    store(file, source, prop, null);
  }

  public void store(Path file, JdbcSource source, Properties prop, StoreCounter counter) throws IOException {
    if (source == null || !source.isReady()) {
      throw new IOException("JDBC Source is not ready");
    }

    JdbcSourceMetadata jdbcMetadata = new JdbcSourceMetadata();
    try {
      jdbcMetadata.setJdbcSource(source);
    } catch (SQLException e) {
      throw new IOException("Can't generate metadata from JDBC source", e);
    }

    int columns = jdbcMetadata.getColumnCount();
    int types[] = jdbcMetadata.types;

    RCFileOutputFormat.setColumnNumber(conf, columns);

    SequenceFile.Metadata metadata = createRCFileMetadata(jdbcMetadata, prop);

    if (codec != null) {
      file = new Path(file.toString()  + codec.getDefaultExtension());
    }

    // Set dump object if counter is null
    if (counter == null) {
      counter = new StoreCounter();
    }

    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null, metadata, codec);
    ResultSet rs = source.getResultSet();
    BytesRefArrayWritable bytes = new BytesRefArrayWritable(columns);

    // TODO Don't break loop while exception threw out.
    // TODO Print whole row data value when an exception raised.
    try {
      while (rs.next()) {
        ++counter.rows;
        for (int c = 0; c < columns; ++c) {
          Object value;
          try {
            value = rs.getObject(c + 1);
            ++counter.cells;
          } catch (SQLException e) {
            throw new IOException("Failed to fetch data from JDBC source, column is "  + jdbcMetadata.names[c] + "/" + c, e);
          }
          bytes.set(c, valueToBytesRef(value, types[c], counter));
        }
        writer.append(bytes);
        bytes.clear();
      }
    } catch (SQLException e) {
      throw new IOException("Failed to fetch data from JDBC source", e);
    }
    writer.close();
  }


  private BytesRefWritable valueToBytesRef(Object value, int c, StoreCounter counter) {
    if (value == null) {
      ++counter.nullCells;
      return stringToBytesRefWritable("NULL");
    }

    BytesRefWritable brw = stringToBytesRefWritable(value.toString());
    counter.bytes += brw.getLength();
    return brw;
  }


  private BytesRefWritable stringToBytesRefWritable(String str) {
    try {
      byte[] bytes = str.getBytes("UTF-8");
      return new BytesRefWritable(bytes, 0, bytes.length);
    } catch (UnsupportedEncodingException e) {
      return stringToBytesRefWritable(NULL_STRING);
    }
  }


  private SequenceFile.Metadata createRCFileMetadata(JdbcSourceMetadata jdbcMetadata, Properties prop) {
    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
    jdbcMetadata.fillRCFileMetadata(metadata);

    metadata.set(new Text("hump.version"), new Text("0.0.1"));
    if (prop != null) {
      Enumeration<Object> keys = prop.keys();
      while (keys.hasMoreElements()) {
        String key = (String) keys.nextElement();
        metadata.set(new Text(key), new Text(prop.getProperty(key)));
      }
    }
    return metadata;
  }
}
