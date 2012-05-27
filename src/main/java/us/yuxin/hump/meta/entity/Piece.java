package us.yuxin.hump.meta.entity;

import javax.jdo.annotations.Index;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.codehaus.jackson.JsonNode;

@Entity
@Table(name = "piece")
public class Piece {
  @Id
  public String id;

  @Index
  public String name;
  public String schema;
  public String category;

  @Index
  public String label1;
  @Index
  public String label2;
  public String label3;
  public String tags;
  public String state;

  public String target;
  public String postfix;
  public int rows;
  public long size;

  @Column(length = 1024)
  public String columns;

  @Column(length = 1024)
  public String hivetypes;

  @Column(length = 1024)
  public String sqltypes;

  public Timestamp created;
  public Timestamp lastUpdate;

  public void loadFromResultSet(ResultSet rs) throws SQLException {
    int o = 0;
    id = rs.getString(++o);
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
    /*
    String query = "SELECT id, name, schema, category, " +
      "label1, label2, label3, tags, state, target, " +
      "rows, size, columns, hivetypes, sqltypes, " +
      "created, lastUpdate FROM piece WHERE id = '" + id + "'";
    */

    String query = "SELECT * FROM piece WHERE id = '" + id + "'";
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
