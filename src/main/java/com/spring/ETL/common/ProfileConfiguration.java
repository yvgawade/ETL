package com.spring.ETL.common;

import java.io.InputStream;
import java.util.Properties;

public class ProfileConfiguration {
  private static Properties pojoprops = null;

  public static Properties getFactoryProperties() {

    if (pojoprops == null) {
      pojoprops = new Properties();
      InputStream is;
      try {
        is = ProfileConfiguration.class.getResourceAsStream("/pojo.properties");
        pojoprops.load(is);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return pojoprops;
  }
}
