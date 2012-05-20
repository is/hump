package us.yuxin.hump.meta.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.codehaus.jackson.JsonNode;

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
  public String target;
  public int rows;
  public long size;
  public String columns;
  public String hivetypes;
  public String sqltypes;
  public Timestamp created;
  public Timestamp lastUpdate;

  public boolean save(Connection co) throws SQLException {

    String updateQuery = "UPDATE piece SET name=?, schema=?, category=?, " +
      "label1=?, label2=?, label3=?, tags=?, state=?, " +
      "target=?, rows=?, size=?, columns=?, hivetypes=?, " +
      "sqltypes=?, lastUpdate=? WHERE id = ?";

    PreparedStatement stmt = co.prepareStatement(updateQuery);
    setParameters(stmt, true);
    stmt.execute();

    if (stmt.getUpdateCount() != 0) {
      stmt.close();
      return false;
    }
    stmt.close();

    String insertQuery = "INSERT INTO piece (name, schema, category, label1, label2, label3, " +
      "tags, state, target, rows, size, columns, hivetypes, sqltypes, created, lastUpdate, id) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    stmt = co.prepareStatement(insertQuery);
    setParameters(stmt, false);
    stmt.execute();
    stmt.close();
    return true;
  }


  private void setParameters(PreparedStatement ps, boolean update) throws SQLException {
    int o = 0;
    ps.setString(++o, name);
    ps.setString(++o, schema);
    ps.setString(++o, category);

    ps.setString(++o, label1);
    ps.setString(++o, label2);
    ps.setString(++o, label3);

    ps.setString(++o, tags);
    ps.setString(++o, state);
    ps.setString(++o, target);

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


  private void loadFromResultSet(ResultSet rs) throws SQLException {
    int o = 0;
    name = rs.getString(++o);
    schema = rs.getString(++o);
    category = rs.getString(++o);
    label1 = rs.getString(++o);
    label2 = rs.getString(++o);
    label3 = rs.getString(++o);
    tags = rs.getString(++o);
    state = rs.getString(++o);
    target = rs.getString(++o);
    rows = rs.getInt(++o);
    size = rs.getLong(++o);
    columns = rs.getString(++o);
    hivetypes = rs.getString(++o);
    sqltypes = rs.getString(++o);
    created = rs.getTimestamp(++o);
    lastUpdate = rs.getTimestamp(++o);
  }


  public boolean load(Connection co, String id) throws SQLException {
    String query = "SELECT name, schema, category, " +
      "label1, label2, label3, tags, state, target, " +
      "rows, size, columns, hivetypes, sqltypes, " +
      "created, lastUpdate FROM piece WHERE id = '" + id + "'";

    Statement stmt = co.createStatement();

    stmt.execute(query);
    ResultSet rs = stmt.getResultSet();
    if (!rs.next()) {
      rs.close();
      stmt.close();
      return false;
    }

    loadFromResultSet(rs);
    this.id = id;
    rs.close();
    stmt.close();
    return true;
  }


  public void loadFromJson(JsonNode node) {
    id = node.get("id").getTextValue();
    String tokens[] = Iterables.toArray(Splitter.on('.').split(id), String.class);
    schema = "log";

    category = tokens[0];
    name = tokens[1];
    label1 = tokens[2];
    label2 = tokens[3];

    state = node.get("status").getTextValue();
    target = node.get("target").getTextValue();

    if (state.equals("OK")) {
      rows = node.get("rows").getIntValue();
      size = node.get("fileBytes").getLongValue();
      columns = node.get("columns").getTextValue();
      hivetypes = node.get("columnTypes").getTextValue();

      created = new Timestamp(System.currentTimeMillis());
      lastUpdate = new Timestamp(System.currentTimeMillis());
    }
  }
}
