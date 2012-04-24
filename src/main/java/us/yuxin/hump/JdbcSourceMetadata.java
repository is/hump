package us.yuxin.hump;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.guava.common.base.Joiner;

public class JdbcSourceMetadata {
  public int columnCount;
  public String names[];
  public String typeNames[];
  public int types[];

	public String columnNames;
	public String columnHiveTypes;


  static ImmutableMap<Integer, String> hiveTypeMap;

  static {
    hiveTypeMap = ImmutableMap.<Integer, String>builder()
      .put(Types.BIT, "boolean")
      .put(Types.BOOLEAN, "boolean")

      .put(Types.TINYINT, "tinyint")
      .put(Types.SMALLINT, "smallint")
      .put(Types.INTEGER, "int")
      .put(Types.BIGINT, "bigint")

      .put(Types.FLOAT, "float")

      .put(Types.NUMERIC, "double")
      .put(Types.DECIMAL, "double")
      .put(Types.REAL, "double")
      .put(Types.DOUBLE, "double")

      .put(Types.DATE, "timestamp")
      .put(Types.TIMESTAMP, "timestamp")

      .put(Types.TIME, "string")
      .put(Types.VARCHAR, "string")
      .put(Types.LONGVARCHAR, "string")
      .put(Types.NVARCHAR, "string")
      .put(Types.NCHAR, "string")
      .put(Types.LONGNVARCHAR, "string")
      .put(Types.CLOB, "string")

      .build();
  }

  /**
   * Initialize Hump metadata by column count.
   *
   * @param count Column count
   */
  public void init(int count) {
    this.columnCount = count;

    this.names = new String[count];
    this.typeNames = new String[count];
    this.types = new int[count];
  }

	public void setJdbcSource(JdbcSource source) throws SQLException {
		ResultSet rs = source.getResultSet();
		if (rs == null)
			return;

		ResultSetMetaData rsmd = rs.getMetaData();
		init(rsmd.getColumnCount());

		for (int c = 0; c < this.columnCount; ++c) {
			this.names[c] = rsmd.getColumnName(c + 1);
			this.types[c] = rsmd.getColumnType(c + 1);
			this.typeNames[c] = rsmd.getColumnTypeName(c + 1);
		}

		this.columnNames = Joiner.on(',').join(this.names);
		this.columnHiveTypes = Joiner.on(':').join(getHiveTypeNames());
	}

  public String[] getHiveTypeNames() {
    String hiveTypeNames[] = new String[columnCount];
    for (int c = 0; c < columnCount; ++c) {
      hiveTypeNames[c] = hiveTypeMap.get(types[c]);
    }
    return hiveTypeNames;
  }


  public void fillRCFileColumns(Properties tbl) {
    tbl.setProperty("columns", columnNames);
    tbl.setProperty("columns.types", columnHiveTypes);
  }


	public void fillRCFileMetadata(SequenceFile.Metadata metadata) {
		metadata.set(new Text("columns"), new Text(columnNames));
		metadata.set(new Text("columns.types"), new Text(columnHiveTypes));
	}

	public int getColumnCount() {
		return columnCount;
	}
}
