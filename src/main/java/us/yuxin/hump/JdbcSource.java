package us.yuxin.hump;

/**
 * Author: scaner@gmail.com
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcSource {
  String driver;
  String url;
  String username;
  String password;

  String query;

  ResultSet resultSet;
  Connection connection;
  Statement statement;

  JdbcSourceMetadata metaData;

  public JdbcSource() {
  }

  /**
   * Open database connection and return query result resultSet.
   *
   * @return result resultSet
   */
  public ResultSet open() throws ClassNotFoundException, SQLException {
    Class.forName(driver);
    connection = DriverManager.getConnection(url, username, password);

    statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    if (driver.equals("com.mysql.jdbc.Driver")) {
      statement.setFetchSize(Integer.MIN_VALUE);
    } else {
      statement.setFetchSize(300);
    }

    statement.execute(query);
    resultSet = statement.getResultSet();
    return resultSet;
  }


  /**
   * Close JDBC Source (result resultSet, statement and connection)
   *
   * @throws SQLException
   */
  public void close() throws SQLException {
    if (resultSet != null) {
      resultSet.close();
      resultSet = null;
    }

    if (statement != null) {
      statement.close();
      statement = null;
    }

    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  public String getDriver() {
    return driver;
  }

  public void setDriver(String driver) {
    this.driver = driver;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }

  public Connection getConnection() {
    return connection;
  }

  public Statement getStatement() {
    return statement;
  }

  public boolean isReady() {
    return (resultSet != null);
  }
}
