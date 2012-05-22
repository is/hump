package us.yuxin.hump.meta;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import com.google.common.collect.Iterables;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import us.yuxin.hump.meta.dao.PieceDao;

public class MetaStore {
  String dbUrl;
  Connection co;


  @SuppressWarnings("unused")
  public Connection getConnection() {
    return co;
  }


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

    // stmt.execute("SET FILES SCRIPT FORMAT COMPRESSED;");
    // stmt.execute("SET DATABASE DEFAULT TABLE TYPE CACHED;");
    // stmt.execute("SET FILES LOG FALSE;");
    // stmt.execute("SET FILES WRITE DELAY 30");

    stmt.addBatch("CREATE TABLE piece (\n" +
      "id VARCHAR(250), -- table id\n" +
      "name VARCHAR(250), -- table name\n" +
      "schema VARCHAR(80), -- log db\n" +
      "category VARCHAR(80), -- game id\n" +
      "label1 VARCHAR(250), -- origin/server id\n" +
      "label2 VARCHAR(40), -- date/other things\n" +
      "label3 VARCHAR(40),\n" +
      "tags VARCHAR(250),\n" +
      "state VARCHAR(40),\n" +
      "target VARCHAR(250),\n" +
      "rows BIGINT,\n" +
      "size BIGINT,\n" +
      "columns VARCHAR(1024),\n" +
      "hivetypes VARCHAR(1024),\n" +
      "sqltypes VARCHAR(1024),\n" +
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
    } else if (url.contains(":h2:")) {
      Class.forName("org.h2.Driver");
    }
  }


  public boolean savePiece(PieceDao piece) throws SQLException {
    boolean ret = piece.save(co);
    co.commit();
    return ret;
  }


  public boolean savePieceWithoutCommit(PieceDao piece) throws SQLException {
    return piece.save(co);
  }


  public boolean loadPiece(PieceDao piece, String id) throws SQLException {
    return piece.load(co, id);
  }


  public void importSummaryLog(BufferedReader br) throws IOException, SQLException {
    ObjectMapper mapper = new ObjectMapper();

    while (true) {
      String line = br.readLine();
      if (line == null)
        break;

      JsonNode node = mapper.readValue(line.trim(), JsonNode.class);
      PieceDao piece = new PieceDao();
      if (node.get("id") == null)
        continue;
      piece.loadFromJson(node);
      if (!piece.state.equals("SKIP")) {
        savePieceWithoutCommit(piece);
      }
    }
    co.commit();
  }


  public void importSummaryLog(String filename) throws IOException, SQLException {
    BufferedReader br = new BufferedReader(new FileReader(filename));
    importSummaryLog(br);
    br.close();
  }


  public void updatePieceStatistic() throws SQLException {
    String columns[] = new String[] {"name", "label1", "label2"};

    Statement stmt = co.createStatement();
    for (String columnName: columns) {
      stmt.execute(String.format("DROP TABLE IF EXISTS stat_%s", columnName));
      stmt.execute(String.format(
        "CREATE TABLE stat_%s AS SELECT %s as key, count(1) as c FROM piece GROUP BY %s;",
        columnName, columnName, columnName));

      // stmt.execute(String.format("CREATE TABLE stat_%s(key VARCHAR(80), c BIGINT);", columnName));
      // stmt.execute(String.format(
      // "INSERT INTO stat_%s (SELECT %s as key, count(1) as c FROM piece GROUP BY %s)",
      //  columnName, columnName, columnName));
    }
    stmt.close();
  }

  public String[] getPieceStatisticByColumn(String column) throws SQLException {
    String query = String.format("SELECT key, c FROM stat_%s ORDER BY key;", column);
    Statement stmt = co.createStatement();
    ResultSet rs = stmt.executeQuery(query);
    LinkedList<String> res = new LinkedList<String>();
    while(rs.next()) {
      res.add(rs.getString(1) + "(" + rs.getInt(2) + ")");
    }
    rs.close();
    stmt.close();

    return Iterables.toArray(res, String.class);
  }
}
