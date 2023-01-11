package com.spring.ETL.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spring.ETL.model.request.IngestDataPOJO;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
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

    return ingestDataPOJO;
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
