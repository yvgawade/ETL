package com.spring.ETL.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.ETL.callable.ProcessData;
import com.spring.ETL.common.POJOFactory;
import com.spring.ETL.entity.IngestDataPOJO;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/hello")
public class IngestDataController {
  private final Logger log = LoggerFactory.getLogger(this.getClass());

  @RequestMapping(method = RequestMethod.POST)
  public String process(@RequestBody String queryChainPOJOString) {

    final IngestDataPOJO ingestDataPOJO = convertToJavaObject(queryChainPOJOString);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(new Runnable() {
      @Override
      public void run() {
        execute(ingestDataPOJO);
      }
    });
    executor.shutdown();
    return "hello " + ingestDataPOJO.getName();
  }

  @Async
  protected IngestDataPOJO execute(final IngestDataPOJO ingestDataPOJO) {
    try {
      ExecutorService executorService = Executors.newFixedThreadPool(1);
      Future<?> futureDataSet = processDataSet(executorService, ingestDataPOJO);
      manageDatasetResults(futureDataSet);
      executorService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ingestDataPOJO;
  }

  protected Future<?> processDataSet(ExecutorService executorService, IngestDataPOJO ingestDataPOJO) {
    Future<?> futureDataSet = null;
    try {
      ProcessData processData = POJOFactory.getProcessor("RDBMS", ingestDataPOJO);
      futureDataSet = executorService.submit(processData);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return futureDataSet;
  }

  protected void manageDatasetResults(Future<?> futureDataSet) {
    if (futureDataSet != null) {
      try {
        IngestDataPOJO pojo = (IngestDataPOJO) futureDataSet.get();
        log.info(pojo.getName());
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  protected IngestDataPOJO convertToJavaObject(String queryChainPOJOString) {

    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readValue(queryChainPOJOString, IngestDataPOJO.class);
    } catch (IOException e1) {
      e1.printStackTrace();
      throw new IllegalArgumentException(e1);
    }
  }
}
