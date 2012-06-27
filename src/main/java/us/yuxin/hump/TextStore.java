package us.yuxin.hump;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

public class TextStore extends StoreBase {
  int rowDelimiter = '\n';
  int fieldDelimiter = '\1';

  public TextStore(FileSystem fs, Configuration conf, CompressionCodec codec) {
    super(fs, conf, codec);
    // TODO row/field delimiter can be set.
  }

  @Override
  public void store(Path file, JdbcSource source, Properties prop, StoreCounter counter) throws IOException {
    setLastRealPath(file);

    if (useTemporary) {
      file = genTempPath();
    }

    if (counter == null) {
      counter = new StoreCounter();
    }

    JdbcSourceMetadata jdbcMetadata = prepareMetadata(source);

    int columns = jdbcMetadata.getColumnCount();
    ResultSet rs = source.getResultSet();

    List<VirtualColumn> virtualColumnList = source.getVirtualColumns();
    int virtualColumnCount = 0;
    VirtualColumn[] virtualColumnArray = null;

    if (virtualColumnList != null) {
      virtualColumnCount = virtualColumnList.size();
      virtualColumnArray = Iterables.toArray(virtualColumnList, VirtualColumn.class);
    }


    System.out.println("TextStore: filename=" + file.toString());
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

          if (c == columns + virtualColumnCount)
            outs.write(rowDelimiter);
          else
            outs.write(fieldDelimiter);
        }

        if (virtualColumnCount != 0) {
          for (int c = 0; c < virtualColumnCount; ++c) {
            outs.write(virtualColumnArray[c].defaultValue.toString().getBytes());
            if (c == virtualColumnCount - 1) {
              outs.write(rowDelimiter);
            } else {
              outs.write(fieldDelimiter);
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to fetch data from JDBC source", e);
    }
    outs.close();
    counter.outBytes = fs.getFileStatus(file).getLen();

    if (useTemporary) {
      renameTemporary();
    }
  }


  @Override
  public String getFormatId() {
    return "text";
  }
}
