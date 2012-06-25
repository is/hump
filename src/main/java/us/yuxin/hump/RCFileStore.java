package us.yuxin.hump;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.Iterables;
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


public class RCFileStore extends StoreBase {
  public final String NULL_STRING = "NULL";


  public RCFileStore(FileSystem fs, Configuration conf, CompressionCodec codec) {
    super(fs, conf, codec);
  }


  @Override
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

    List<VirtualColumn> virtualColumnList = source.getVirtualColumns();
    int virtualColumnCount = 0;
    VirtualColumn[] virtualColumnArray = null;

    if (virtualColumnList != null) {
      virtualColumnCount = virtualColumnList.size();
      virtualColumnArray = Iterables.toArray(virtualColumnList, VirtualColumn.class);
    }

    RCFileOutputFormat.setColumnNumber(conf, columns + virtualColumnCount);
    SequenceFile.Metadata metadata = createRCFileMetadata(
      source, jdbcMetadata, prop);

    setLastRealPath(file);
    if (useTemporary) {
      file = genTempPath();
    }

    // Set dump object if counter is null
    if (counter == null) {
      counter = new StoreCounter();
    }

    RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null, metadata, codec);
    ResultSet rs = source.getResultSet();
    BytesRefArrayWritable bytes = new BytesRefArrayWritable(columns + virtualColumnCount);

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
            throw new IOException("Failed to fetch data from JDBC source, column is " + jdbcMetadata.names[c] + "/" + c, e);
          }
          bytes.set(c, valueToBytesRef(value, types[c], counter));
        }

        if (virtualColumnCount != 0) {
          for (int c = 0; c < virtualColumnCount; ++c) {
            byte[] barray = virtualColumnArray[c].defaultValue.toString().getBytes();
            bytes.set(columns + c, new BytesRefWritable(barray, 0, barray.length));
          }
        }

        writer.append(bytes);
        bytes.clear();
      }
    } catch (SQLException e) {
      throw new IOException("Failed to fetch data from JDBC source", e);
    }

    writer.close();
    counter.outBytes = fs.getFileStatus(file).getLen();

    if (useTemporary) {
      fs.mkdirs(getLastRealPath().getParent());
      fs.rename(getLastTempPath(), getLastRealPath());
    }
  }


  private BytesRefWritable valueToBytesRef(Object value, int c, StoreCounter counter) {
    if (value == null) {
      ++counter.nullCells;
      return stringToBytesRefWritable("NULL");
    }

    BytesRefWritable brw = stringToBytesRefWritable(value.toString());
    counter.inBytes += brw.getLength();
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


  public static SequenceFile.Metadata createRCFileMetadata(
    JdbcSource source, JdbcSourceMetadata jdbcMetadata, Properties prop) {
    SequenceFile.Metadata metadata = new SequenceFile.Metadata();

    metadata.set(new Text("hump.version"), new Text("0.0.1"));

    String names = jdbcMetadata.columnNames;
    String types = jdbcMetadata.columnHiveTypes;
    int columns = jdbcMetadata.columnCount;

    List<VirtualColumn> virtualColumns = source.getVirtualColumns();
    if (virtualColumns != null) {
      for (VirtualColumn vc : virtualColumns) {
        ++columns;
        names += "," + vc.columnName;
        types += ":" + vc.columnType;
      }
    }

    metadata.set(new Text("columns"), new Text(names));
    metadata.set(new Text("columns.types"), new Text(types));

    if (prop != null) {
      Enumeration<Object> keys = prop.keys();
      while (keys.hasMoreElements()) {
        String key = (String) keys.nextElement();
        metadata.set(new Text(key), new Text(prop.getProperty(key)));
      }
    }

    return metadata;
  }


//  private SequenceFile.Metadata createRCFileMetadata2(
//    JdbcSource source, JdbcSourceMetadata jdbcMetadata, Properties prop) {
//    SequenceFile.Metadata metadata = new SequenceFile.Metadata();
//
//    jdbcMetadata.fillRCFileMetadata(metadata);
//
//    metadata.set(new Text("hump.version"), new Text("0.0.1"));
//    if (prop != null) {
//      Enumeration<Object> keys = prop.keys();
//      while (keys.hasMoreElements()) {
//        String key = (String) keys.nextElement();
//        metadata.set(new Text(key), new Text(prop.getProperty(key)));
//      }
//    }
//    return metadata;
//  }


  public String getFormatId() {
    return "rcfile";
  }
}
