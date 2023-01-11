package com.spring.ETL.model.request;

public class IngestDataPOJO {
  String name;

  public IngestDataPOJO() {
  }

  public IngestDataPOJO(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
