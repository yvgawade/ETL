package com.spring.ETL.callable;

import com.spring.ETL.common.ProfileConfiguration;
import com.spring.ETL.entity.IngestDataPOJO;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestDataByPackets extends ProcessData<IngestDataPOJO> {
  private final Logger log = LoggerFactory.getLogger(this.getClass());
  private IngestDataPOJO ingestDataPOJO;
  private Connection localConn;
  private Connection h2Conn;

  private Connection remoteConn;

  public IngestDataByPackets() {

  }

  public IngestDataByPackets(IngestDataPOJO ingestDataPOJO) {

    this.ingestDataPOJO = ingestDataPOJO;
  }

  @Override
  public IngestDataPOJO call() throws Exception {

    execute();
    log.info("hello " + ingestDataPOJO.getName());
    ingestDataPOJO.setName("hello from ingest data");
    return ingestDataPOJO;
  }


  private void execute() {
    int packetsSize = 100000;
    Connection conn = null;
    ResultSet res = null;
    long start1 = System.currentTimeMillis();
    try {
      String targetFileName = rd_home + File.separator + UUID.randomUUID().toString().replaceAll("-", "_") + ".backticksv";

      ProfileConfiguration conf = new ProfileConfiguration();
      remoteConn = getRemoteDbConnection(ingestDataPOJO.getDatabaseUrl(), ingestDataPOJO.getUsername(), ingestDataPOJO.getPassword(), ingestDataPOJO.getDriverClass());
      localConn = getRemoteDbConnection(conf.ingestDbUrl, conf.ingestDbUsername, conf.ingestDbPassword, conf.ingestDriverClass);
      h2Conn = getRemoteDbConnection(conf.h2DbUrl, conf.h2DbUsername, conf.h2DbPassword, conf.h2DriverClass);

      int rowCount = (int) getResults(remoteConn, "SELECT COUNT(*) FROM " + ingestDataPOJO.getTableName());
      int packetsCount = rowCount % packetsSize == 0 ? rowCount / packetsSize : (rowCount / packetsSize) + 1;
      final ExecutorService pool = Executors.newFixedThreadPool(packetsCount);
      final ExecutorCompletionService<String> packetsPool = new ExecutorCompletionService<>(pool);

      String remoteQuery = getRemoteQuery(ingestDataPOJO.getTableName(), ingestDataPOJO.getColumns());
      res = selectFromRemoteTable(remoteConn, remoteQuery + " WHERE 1=0");
      ResultSetMetaData rsmdt = res.getMetaData();

      String targetDBTable = conf.ingestDbName + "." + "DI_71";
      String tableDef = getIngestTableDef(rsmdt, "`");
      String h2TableDef = getIngestTableDef(rsmdt, "\"");
      ingestPackets(h2TableDef, remoteQuery + " LIMIT " + packetsSize + " OFFSET 0", targetDBTable, 0, targetFileName);
      if (packetsCount > 1) {
        for (int i = 2; i <= packetsCount; i++) {
          int offset = (i - 1) * packetsSize;
          final String finalQuery = remoteQuery + " LIMIT " + packetsSize + " OFFSET " + offset;
          final int packetIdx = i;
          packetsPool.submit(new Callable<String>() {
            @Override
            public String call()  {
              return ingestPackets(h2TableDef, finalQuery, targetDBTable, packetIdx, targetFileName);
            }
          });
        }
        for (int i = 2; i < packetsCount; i++)
          log.info(packetsPool.take().get() + " packet completed.");
        pool.shutdown();
      }

      createIngestDataTable(localConn,  normalizeTableName(targetDBTable), tableDef);
      bulkInsert(localConn,  normalizeTableName(targetDBTable), targetFileName, "`");
      long end1 = System.currentTimeMillis();
      log.info("total time " + (end1-start1));
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      if (res != null) {
        try {
          res.close();
        } catch (Exception ignored) {
        }
      }
      if (remoteConn != null) {
        try {
          conn.close();
        } catch (Exception ignored) {
        }
      }
      if (localConn != null) {
        try {
          localConn.close();
        } catch (Exception ignored) {
        }
      }
      if (h2Conn != null) {
        try {
          h2Conn.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  private void  applyTransformation(String tableDef, String fileName, String tableName, String targetFIle) {
    ResultSet res = null;
    try {
      bulkInsertIntoH2(h2Conn, tableDef, fileName, tableName);
      String remoteQuery = "SELECT * FROM " + normalizeH2TableName(tableName) + " WHERE " + ingestDataPOJO.getFilter();
      res = selectFromRemoteTable(h2Conn, remoteQuery);
      ResultSetMetaData rsmdt = res.getMetaData();
      String dropQuery = "DROP TABLE IF EXISTS " + normalizeH2TableName(tableName);
      appendToFile(res, rsmdt, targetFIle);
      int rowCount = (int) getResults(h2Conn, "SELECT COUNT(*) FROM " + normalizeH2TableName(tableName));
      log.info(rowCount + " records transformed");
      executeQuery(h2Conn, dropQuery);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        if (res != null) {
          res.close();
        }
      } catch (SQLException ignored) {

      }
    }
  }

  public String ingestPackets(String tableDef, String remoteQuery, String targetDbTable, int packetIdx, String targetFile) {
    try {
      ResultSet res = selectFromRemoteTable(remoteConn, remoteQuery);
      ResultSetMetaData rsmdt = res.getMetaData();
      String fileName = createCSVDump(res, rsmdt);
      applyTransformation(tableDef, fileName, targetDbTable + "_" + packetIdx, targetFile);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return String.valueOf(packetIdx);
  }

  public long getResults(Connection con, String sql) throws SQLException {

    Statement stmt = null;
    ResultSet res = null;
    try {
      stmt = con.createStatement();
      res = stmt.executeQuery(sql);
      long count = 0;
      if (res.next()) {
        count = res.getLong(1);
      }
      res.close();
      stmt.close();
      return count;
    } catch (SQLException e) {
      log.info("SQLException for sql: " + sql);
      e.printStackTrace();
      String table = "";
      if (sql.startsWith("SELECT COUNT(*) FROM")) {
        table = table + sql.substring("SELECT COUNT(*) FROM".length());
      }
      throw new SQLException("Table doesn't exist: " + table, e);
    } finally {
      try {
        if (res != null)
          res.close();
        if (stmt != null)
          stmt.close();
      } catch (SQLException ignored) {

      }
    }
  }

  public ResultSet selectFromRemoteTable(Connection remoteConn, String sourceTableOrQuery) throws SQLException {

    Statement stmt;
    ResultSet res;
    log.info("Remote query: " + sourceTableOrQuery);
    try {
      stmt = remoteConn.createStatement();
      stmt.setFetchSize(2000);
      res = stmt.executeQuery(sourceTableOrQuery);
    } catch (SQLException e) {
      e.printStackTrace();
      throw e;
    }
    return res;
  }

  private void executeQuery(Connection con, String sql) {
    Statement stmt = null;
    ResultSet res = null;
    try {
      stmt = con.createStatement();
      stmt.executeUpdate(sql);
      stmt.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        if (res != null)
          res.close();
        if (stmt != null)
          stmt.close();
      } catch (SQLException ignored) {

      }
    }
  }
}
