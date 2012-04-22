package us.yuxin.hump;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.After;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class JdbcSourceTest {
  Connection connection;
  static String driver = "org.hsqldb.jdbcDriver";
  static String url = "jdbc:hsqldb:mem:UnitTestJdbcSource";

  @Before
  public void setUp() {
    try {
      Class.forName(driver);
      connection = DriverManager.getConnection(url);

      Statement stmt = connection.createStatement();
      stmt.execute("CREATE TABLE t0 (" +
        "a0 BIGINT," +
        "b0 BIGINT)");
      stmt.execute("INSERT INTO t0 VALUES (1, 2)");
      stmt.execute("INSERT INTO t0 VALUES (3, 4)");
      stmt.close();
      connection.commit();
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during HSQL database startup.");
    }
  }

  @After
  public void tearDown() {
    try {
      connection.createStatement().execute("SHUTDOWN");
    } catch (Exception e) {
      e.printStackTrace();
      fail("Shutdown HSQL database");
    }
  }

  @Test
  public void testOpenAndClose() {
    try {
      JdbcSource source = new JdbcSource();
      source.setDriver(driver);
      source.setUrl(url);
      source.setUsername("sa");
      source.setQuery("SELECT * FROM t0");
      source.open();

      ResultSet rs = source.getResultSet();
      source.close();

    } catch (Exception e) {
      e.printStackTrace();
      fail("SQL Exception is throw out.");
    }
  }
}
