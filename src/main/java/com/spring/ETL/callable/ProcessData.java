package com.spring.ETL.callable;

import com.spring.ETL.entity.CommonPOJO;
import java.util.concurrent.Callable;


public abstract class ProcessData<T> implements Callable<CommonPOJO<T>> {
  public CommonPOJO<T> call() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }
}
