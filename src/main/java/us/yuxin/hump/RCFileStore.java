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

		public RCFileStore(FileSystem fs) {
		this.fs = fs;
	}


	public void store(Path file, JdbcSource source, Properties prop) throws IOException {
		if (source == null || !source.isReady()) {
			throw new IOException("JDBC Source is not ready");
		}

		JdbcSourceMetadata jdbcMetadata = new JdbcSourceMetadata();
		try {
			jdbcMetadata.setJdbcSource(source);
		} catch (SQLException e) {
			throw new IOException("Can't generate metadata from JDBC source", e);
		}

		SequenceFile.Metadata metadata = createRCFileMetadata(jdbcMetadata, prop);
		RCFile.Writer writer = new RCFile.Writer(fs, conf, file, null, metadata, codec);

		ResultSet rs = source.getResultSet();

		int columns = jdbcMetadata.getColumnCount();
		int types[] = jdbcMetadata.types;

		BytesRefArrayWritable bytes = new BytesRefArrayWritable(columns);

		try {
			while(rs.next()) {
				for (int c = 0; c < columns; ++c) {
					Object value = rs.getObject(c + 1);
					bytes.set(c, valueToBytesRef(value, types[c]));
				}
			}
			writer.append(bytes);
			bytes.clear();
		} catch (SQLException e) {
			throw new IOException("Failed to fetch data from JDBC source", e);
		}
		writer.close();
	}


	private BytesRefWritable valueToBytesRef(Object value, int c) {
		if (value == null)
			return stringToBytesRefWritable("NULL");
		return stringToBytesRefWritable(value.toString());
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
			while(keys.hasMoreElements()) {
				String key = (String)keys.nextElement();
				metadata.set(new Text(key), new Text(prop.getProperty(key)));
			}
		}
		return metadata;
	}
}
