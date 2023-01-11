package com.spring.ETL.callable;

import com.spring.ETL.entity.IngestDataPOJO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestData extends ProcessData<IngestDataPOJO> {

  private final Logger log = LoggerFactory.getLogger(this.getClass());
  IngestDataPOJO ingestDataPOJO;


  public IngestData() {

  }

  public IngestData(IngestDataPOJO ingestDataPOJO) {
    this.ingestDataPOJO = ingestDataPOJO;
  }

  @Override
  public IngestDataPOJO call() throws Exception {
    log.info("hello " + ingestDataPOJO.getName());
    ingestDataPOJO.setName("hello from ingest data");
    return ingestDataPOJO;
  }
}
