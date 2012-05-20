package us.yuxin.hump.meta.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class PieceDao {
  public String id;
  public String name;
  public String schema;
  public String category;
  public String label1;
  public String label2;
  public String label3;
  public String tags;
  public String state;
  public int rows;
  public long size;
  public String columns;
  public String hivetypes;
  public String sqltypes;
  public Timestamp created;
  public Timestamp lastUpdate;


  public void setParameters(PreparedStatement ps, boolean update) throws SQLException {
    int o = 0;
    ps.setString(++o, name);
    ps.setString(++o, schema);
    ps.setString(++o, category);

    ps.setString(++o, label1);
    ps.setString(++o, label2);
    ps.setString(++o, label3);

    ps.setString(++o, tags);
    ps.setString(++o, state);

    ps.setInt(++o, rows);
    ps.setLong(++o, size);

    ps.setString(++o, columns);
    ps.setString(++o, hivetypes);
    ps.setString(++o, sqltypes);

    if (!update)
      ps.setTimestamp(++o, created);
    ps.setTimestamp(++o, lastUpdate);

    ps.setString(++o, id);
  }


  public void getParameters(ResultSet rs) throws SQLException {
    int o = 0;
    name = rs.getString(++o);
    schema = rs.getString(++o);
    category = rs.getString(++o);
    label1 = rs.getString(++o);
    label2 = rs.getString(++o);
    label3 = rs.getString(++o);
    tags = rs.getString(++o);
    state = rs.getString(++o);
    rows = rs.getInt(++o);
    size = rs.getLong(++o);
    columns = rs.getString(++o);
    hivetypes = rs.getString(++o);
    sqltypes = rs.getString(++o);
    created = rs.getTimestamp(++o);
    lastUpdate = rs.getTimestamp(++o);
  }
}
