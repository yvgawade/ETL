package com.spring.ETL.common;

import java.io.InputStream;
import java.util.Properties;

public class ProfileConfiguration {
  private static Properties props = null;
  public final String ingestDbName = getProperties().getProperty("etl.connection.db");
  public final String ingestDbUrl = getProperties().getProperty("etl.connection.url");
  public final String ingestDriverClass = getProperties().getProperty("etl.connection.driver_class");
  public final String ingestDbUsername = getProperties().getProperty("etl.connection.username");
  public final String ingestDbPassword = getProperties().getProperty("etl.connection.password");
  public final String h2DbUrl = "jdbc:h2:mem:\"" + ingestDbName  + "\";LOCK_TIMEOUT=30000;MODE=MSSQLServer;" +
      "INIT=CREATE SCHEMA IF NOT EXISTS \"" + ingestDbName + "\"\\;" +
      "SET SCHEMA \"" + ingestDbName + "\"";
  public final String h2DriverClass = getProperties().getProperty("h2.connection.driver_class");
  public final String h2DbUsername = getProperties().getProperty("h2.connection.username");
  public final String h2DbPassword = getProperties().getProperty("h2.connection.password");

  public static Properties getProperties() {

    if (props == null) {
      props = new Properties();
      InputStream is;
      try {
        is = ProfileConfiguration.class.getResourceAsStream("/profile.properties");
        props.load(is);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return props;
  }

}
