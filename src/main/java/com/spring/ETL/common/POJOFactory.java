package com.spring.ETL.common;

import com.spring.ETL.callable.ProcessData;
import com.spring.ETL.entity.CommonPOJO;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class POJOFactory {
  public final static Properties POJOFACTORY = ProfileConfiguration.getProperties();


  public static ProcessData getProcessor(String type, CommonPOJO<?> pojo) {

    String processorClassName = POJOFACTORY.get(type.toUpperCase() + "_P").toString();
    try {
      Class clazz = Class.forName(processorClassName);
      return (ProcessData) clazz.getConstructor(pojo.getClass()).newInstance(pojo);
    } catch (ClassNotFoundException | IllegalArgumentException | InstantiationException |
             NoSuchMethodException | SecurityException | IllegalAccessException |
             InvocationTargetException e) {
      e.printStackTrace();
    }

    return null;

  }
}
