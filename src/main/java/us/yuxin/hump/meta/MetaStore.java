package us.yuxin.hump.meta;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import us.yuxin.hump.meta.dao.PieceDao;

public class MetaStore {
  String dbUrl;
  Connection co;


  public MetaStore() {
  }


  /**
   * Open existed metastore.
   *
   * @param url Datasource JDBC URL.
   */
  public void open(String url) throws SQLException, ClassNotFoundException {
    this.dbUrl = url;
    prepareJDBCDriver(url);
    co = DriverManager.getConnection(url);
  }


  public void close() throws SQLException {
    if (co != null) {
      co.commit();
      co.close();
    }
  }


  public void createSchema() throws SQLException {
    createSchemaForHsqldb(co);
  }


  public static void createSchemaForHsqldb(Connection co) throws SQLException {
    Statement stmt = co.createStatement();

    stmt.execute("SET FILES SCRIPT FORMAT COMPRESSED;");

    stmt.addBatch("CREATE TABLE piece (\n" +
      "id VARCHAR(250), -- table id\n" +
      "name VARCHAR(250), -- table name\n" +
      "schema VARCHAR(80), -- log db\n" +
      "category VARCHAR(80), -- game id\n" +
      "label1 VARCHAR(250), -- origin/server id\n" +
      "label2 VARCHAR(40), -- date/other things\n" +
      "label3 VARCHAR(40),\n" +
      "tags VARCHAR(250),\n" +
      "rows BIGINT,\n" +
      "size BIGINT,\n" +
      "columns CLOB,\n" +
      "hivetypes CLOB,\n" +
      "sqltypes CLOB,\n" +
      "created TIMESTAMP,\n" +
      "lastUpdate TIMESTAMP,\n" +
      "PRIMARY KEY(id)\n" +
      ");");

    stmt.addBatch("CREATE INDEX piece__name ON piece(name);");
    stmt.addBatch("CREATE INDEX piece__category ON piece(category);");
    stmt.addBatch("CREATE INDEX piece__label1 ON piece(label1);");
    stmt.addBatch("CREATE INDEX piece__label2 ON piece(label2);");

    stmt.executeBatch();
    stmt.close();
  }


  public static void prepareJDBCDriver(String url) throws ClassNotFoundException {
    if (url.contains(":mysql:")) {
      Class.forName("com.mysql.jdbc.Driver");
    } else if (url.contains(":hsqldb:")) {
      Class.forName("org.hsqldb.jdbcDriver");
    }
  }


  public boolean load(PieceDao piece, String id) throws SQLException {
    String query = "SELECT name, schema, category, " +
      "label1, label2, label3, tags," +
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

    piece.getParameters(rs);
    piece.id = id;
    rs.close();
    stmt.close();
    return true;
  }

  public boolean save(PieceDao piece) throws SQLException {

    String updateQuery = "UPDATE piece SET name=?, schema=?, category=?, " +
      "label1=?, label2=?, label3=?, tags=?, " +
      "rows=?, size=?, columns=?, hivetypes=?, " +
      "sqltypes=?, lastUpdate=? WHERE id = ?";

    PreparedStatement stmt = co.prepareStatement(updateQuery);
    piece.setParameters(stmt, true);
    stmt.execute();

    if (stmt.getUpdateCount() != 0) {
      stmt.close();
      co.commit();
      return false;
    }
    stmt.close();

    String insertQuery = "INSERT INTO piece (name, schema, category, label1, label2, label3, " +
      "tags, rows, size, columns, hivetypes, sqltypes, created, lastUpdate, id) " +
      "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    stmt = co.prepareStatement(insertQuery);
    piece.setParameters(stmt, false);
    stmt.execute();
    stmt.close();
    co.commit();

    return true;
  }
}
