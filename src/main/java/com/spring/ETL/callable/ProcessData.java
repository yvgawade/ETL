package com.spring.ETL.callable;

import com.spring.ETL.entity.CommonPOJO;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class ProcessData<T> implements Callable<CommonPOJO<T>> {

  final String rd_home = System.getenv("CSV_Dump");

  private final Logger log = LoggerFactory.getLogger(this.getClass());

  public Connection getRemoteDbConnection(String connectionJDBCURL, String username, String password, String driverClass)
      throws SQLException {
    Connection conn = null;
    try {

      try {
        Class.forName(driverClass);
      } catch (ClassNotFoundException cnf) {
        log.info("driver could not be loaded: " + cnf);
      }
      conn = DriverManager.getConnection(connectionJDBCURL, username, password);
    } catch (SQLException e) {
      e.printStackTrace();
      log.info(String.valueOf(e.getCause()));
      throw e;
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      log.info(String.valueOf(e.getCause()));
      throw e;
    }
    return conn;
  }

  public String mapToMySQLDataType(String dataType) {
    if (dataType.startsWith("VARCHAR") || dataType.startsWith("CHAR"))
      return "VARCHAR(255)";
    return dataType;
  }

  public String getIngestTableDef(ResultSetMetaData rsmdt, String escape) {

    StringBuilder tableDef = new StringBuilder();
    try {
      int columnCount = rsmdt.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = rsmdt.getColumnName(i);
        String dataType = rsmdt.getColumnTypeName(i);
        tableDef.append(escape).append(columnName).append(escape).append(" ").append(mapToMySQLDataType(dataType)).append(", ");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    if (tableDef.toString().endsWith(", "))
      tableDef = new StringBuilder(tableDef.substring(0, tableDef.length() - 2));
    return tableDef.toString();
  }

  public void createIngestDataTable(Connection con, String tableName, String columns) throws SQLException {

    Statement statement = null;
    try {
      statement = con.createStatement();
      StringBuilder sql = new StringBuilder(1024);
      sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (").append(columns).append(")");
      log.info(sql.toString());
      statement.execute(sql.toString());
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException ex) {

        }
      }
    }
  }

  public void createIngestDataTableAs(Connection con, String tableName, String createQuery) throws SQLException {

    Statement statement = null;
    try {
      statement = con.createStatement();
      StringBuilder sql = new StringBuilder(1024);
      sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" AS ").append(createQuery).append("");
      log.info(sql.toString());
      statement.execute(sql.toString());
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException ex) {

        }
      }
    }
  }

  public void appendToFile(ResultSet res, ResultSetMetaData rsmdt, String fileName) {

    try {
      StringBuilder stb = new StringBuilder(1024);
      File tmp = new File(fileName);
      tmp.getParentFile().mkdirs();
      if (!tmp.exists())
        tmp.createNewFile();
      OutputStream out = new FileOutputStream(tmp, true);
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
      int columnCount = rsmdt.getColumnCount();
      while (res.next()) {
        stb.setLength(0);
        for (int i = 1; i <= columnCount; i++) {
          if (res.getString(i) != null) {
            stb.append(res.getString(i));
            stb.append("`");
          } else
            stb.append("`");
        }
        bw.write(stb.toString());
        bw.write(System.lineSeparator());
      }
      bw.flush();
      bw.close();
      out.close();
    } catch (IOException | SQLException e) {
      e.printStackTrace();
    }
  }

  public String createCSVDump(ResultSet res, ResultSetMetaData rsmdt) {
    String fileName = rd_home + File.separator + UUID.randomUUID().toString().replaceAll("-", "_") + ".backticksv";
    appendToFile(res, rsmdt, fileName);
    return fileName;
  }

  public void bulkInsert(Connection con, String tableName, String fileName, String separator) throws SQLException {

    fileName = fileName.replaceAll("\\\\", "/");
    StringBuilder sql = new StringBuilder();
    sql.append("LOAD DATA LOCAL INFILE '")
        .append(fileName)
        .append("' INTO TABLE ")
        .append(tableName)
        .append(" CHARACTER SET Latin1 FIELDS TERMINATED BY '`'")
        .append(" LINES TERMINATED BY '`\\r\\n'");
    log.info(sql.toString());
    Statement stmt = null;
    try {
      stmt = con.createStatement();
      stmt.executeUpdate("SET GLOBAL local_infile=1");
      stmt.executeUpdate(sql.toString());
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {

        }
      }
    }
  }

  public void bulkInsertIntoH2(Connection h2Con, String tableDef, String file, String table) throws SQLException {

    java.nio.file.Path path = Paths.get(file);
    StringBuilder query = new StringBuilder("CREATE TABLE ").append(normalizeH2TableName(table)).append("(").append(tableDef).append(") AS SELECT * FROM CSVRead('").append(path.toString()).append("',NULL,'fieldSeparator=` fieldDelimiter=')");
    log.info(query.toString());
    try {
      h2Con.setAutoCommit(false);
      Statement stmt = h2Con.createStatement();
      stmt.executeUpdate(query.toString());
      h2Con.commit();
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void bulkAppendToH2(Connection h2Con, String tableDef, String file, String table) throws SQLException {
    Path path = Paths.get(file);
    StringBuilder query = new StringBuilder("INSERT INTO ").append(normalizeH2TableName(table)).append("(").append(tableDef).append(") SELECT * FROM CSVRead('").append(path.toString()).append("',NULL,'charset=UTF-8 fieldSeparator=` fieldDelimiter=')");
    log.info(query.toString());
    try {
      h2Con.setAutoCommit(false);
      Statement stmt = h2Con.createStatement();
      stmt.executeUpdate(query.toString());
      h2Con.commit();
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    }
  }



  public String getRemoteQuery(String tableName, String columns) {
    return "SELECT * FROM " + normalizeTableName(tableName);
  }

  public String normalizeH2TableName(String tableName) {

    int index = tableName.indexOf(".");
    tableName = "\"" + tableName.substring(0, index) + "\"." + tableName.substring(index + 1);
    return tableName;
  }

  public String normalizeTableName(String tableName) {
    String[] strArr = tableName.split("\\.");
    if (strArr.length > 1)
      return strArr[0] + ".`" + strArr[1] + "`";
    return tableName;
  }
  public CommonPOJO<T> call() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }
}
