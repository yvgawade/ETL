package com.spring.ETL.callable;

import com.spring.ETL.common.ProfileConfiguration;
import com.spring.ETL.entity.IngestDataPOJO;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestData extends ProcessData<IngestDataPOJO> {

  private final Logger log = LoggerFactory.getLogger(this.getClass());

  private IngestDataPOJO ingestDataPOJO;
  private Connection localConn;

  private Connection h2Conn;
  public IngestData() {

  }
  public IngestData(IngestDataPOJO ingestDataPOJO) {

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

  }

  public void insertIntoTable() {
    Connection conn = null;
    Statement stmt = null;
    ResultSet res = null;
    try {
      long start1 = System.currentTimeMillis();
      ProfileConfiguration conf = new ProfileConfiguration();
      conn = getRemoteDbConnection(ingestDataPOJO.getDatabaseUrl(), ingestDataPOJO.getUsername(), ingestDataPOJO.getPassword(), ingestDataPOJO.getDriverClass());
      localConn = getRemoteDbConnection(conf.ingestDbUrl, conf.ingestDbUsername, conf.ingestDbPassword, conf.ingestDriverClass);
      h2Conn = getRemoteDbConnection(conf.h2DbUrl, conf.h2DbUsername, conf.h2DbPassword, conf.h2DriverClass);

      String remoteQuery = getRemoteQuery(ingestDataPOJO.getTableName(), ingestDataPOJO.getColumns());
      stmt = conn.createStatement();
      res = stmt.executeQuery(remoteQuery);
      ResultSetMetaData rsmdt = res.getMetaData();

      String targetDBTable = conf.ingestDbName + "." + "DI_8";
      String tableDef = getIngestTableDef(rsmdt, "`");
      String fileName = createCSVDump(res, rsmdt);
      long end2 = System.currentTimeMillis();
      log.info("CSV DUMP " +  (end2 - start1));
      fileName = applyTransformation(tableDef, fileName, targetDBTable);
      long end3 = System.currentTimeMillis();
      log.info("TRANSFORMATION " + (end3 - start1));
      createIngestDataTable(localConn,  normalizeTableName(targetDBTable), tableDef);
      bulkInsert(localConn,  normalizeTableName(targetDBTable), fileName, "`");
      long end4 = System.currentTimeMillis();
      log.info("BULK INSERT " + (end4 - start1));
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (res != null) {
        try {
          res.close();

        } catch (Exception ignored) {
        }
      }
      if (stmt != null) {
        try {
          stmt.close();
        } catch (Exception ignored) {
        }
      }
      if (conn != null) {
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

  private String applyTransformation(String tableDef, String fileName, String tableName) throws SQLException {

    bulkInsertIntoH2(h2Conn, tableDef, fileName, tableName);
    String remoteQuery = "SELECT * FROM " + normalizeH2TableName(tableName) + " WHERE " + ingestDataPOJO.getFilter();
    Statement stmt = h2Conn.createStatement();
    ResultSet res = stmt.executeQuery(remoteQuery);
    ResultSetMetaData rsmdt = res.getMetaData();
    return createCSVDump(res, rsmdt);
  }
}
