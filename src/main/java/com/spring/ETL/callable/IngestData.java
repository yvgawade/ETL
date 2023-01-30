package com.spring.ETL.callable;

import com.spring.ETL.common.ProfileConfiguration;
import com.spring.ETL.entity.IngestDataPOJO;
import com.spring.ETL.util.IngestDataUtils;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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


  private void execute() {

    Connection conn = null;
    Statement stmt = null;
    ResultSet res = null;
    try {
      long start1 = System.currentTimeMillis();
      ProfileConfiguration conf = new ProfileConfiguration();
      IngestDataUtils ingestDataUtils = new IngestDataUtils();
      conn = ingestDataUtils.getRemoteDbConnection(ingestDataPOJO.getDatabaseUrl(), ingestDataPOJO.getUsername(), ingestDataPOJO.getPassword(), ingestDataPOJO.getDriverClass());
      localConn = ingestDataUtils.getRemoteDbConnection(conf.ingestDbUrl, conf.ingestDbUsername, conf.ingestDbPassword, conf.ingestDriverClass);
      h2Conn = ingestDataUtils.getRemoteDbConnection(conf.h2DbUrl, conf.h2DbUsername, conf.h2DbPassword, conf.h2DriverClass);

      String remoteQuery = ingestDataUtils.getRemoteQuery(ingestDataPOJO.getTableName(), ingestDataPOJO.getColumns());
      stmt = conn.createStatement();
      res = stmt.executeQuery(remoteQuery);
      ResultSetMetaData rsmdt = res.getMetaData();

      String targetDBTable = conf.ingestDbName + "." + "DI_8";
      String tableDef = ingestDataUtils.getIngestTableDef(rsmdt);
      String fileName = ingestDataUtils.createCSVDump(res, rsmdt);
      long end2 = System.currentTimeMillis();
      log.info("CSV DUMP " +  (end2 - start1));
      fileName = applyTransformation(tableDef, fileName, targetDBTable);
      long end3 = System.currentTimeMillis();
      log.info("TRANSFORMATION " + (end3 - start1));
      ingestDataUtils.createIngestDataTable(localConn,  ingestDataUtils.normalizeTableName(targetDBTable), tableDef);
      ingestDataUtils.bulkInsert(localConn,  ingestDataUtils.normalizeTableName(targetDBTable), fileName, "`");
      long end4 = System.currentTimeMillis();
      log.info("BULK INSERT " + (end4 - start1));

      //String createQuery = "SELECT * FROM " +  ingestDataUtils.normalizeTableName(targetDBTable) + " WHERE " + ingestDataPOJO.getFilter();
      //ingestDataUtils.createIngestDataTableAs(localConn, ingestDataUtils.normalizeTableName(targetDBTable + "_1"), createQuery);
      //long end5 = System.currentTimeMillis();
      //log.info("Transformation " + (end5 - start1));
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (res != null) {
        try {
          res.close();

        } catch (Exception e) {
        }
      }
      if (stmt != null) {
        try {
          stmt.close();
        } catch (Exception e) {
        }
      }
      if (conn != null) {
        try {
          conn.close();
        } catch (Exception e) {
        }
      }
      if (localConn != null) {
        try {
          localConn.close();
        } catch (Exception e) {
        }
      }
    }
  }

  private String applyTransformation(String tableDef, String fileName, String tableName)
      throws SQLException {
    IngestDataUtils ingestDataUtils = new IngestDataUtils();
    ingestDataUtils.bulkInsertIntoH2(h2Conn, tableDef, fileName, tableName);
    String remoteQuery = "SELECT * FROM " + ingestDataUtils.normalizeH2TableName(tableName) + " WHERE " + ingestDataPOJO.getFilter();
    Statement stmt = h2Conn.createStatement();
    ResultSet res = stmt.executeQuery(remoteQuery);
    ResultSetMetaData rsmdt = res.getMetaData();
    return ingestDataUtils.createCSVDump(res, rsmdt);
  }

  @Override
  public IngestDataPOJO call() throws Exception {
    execute();
    log.info("hello " + ingestDataPOJO.getName());
    ingestDataPOJO.setName("hello from ingest data");
    return ingestDataPOJO;
  }
}
