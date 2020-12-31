package com.basic.core.Component;

import org.apache.storm.tuple.Tuple;

public class SortedTuple {
  private Tuple tuple;
  private Long ts;

  public SortedTuple(Tuple tuple, Long ts) {
    this.tuple = tuple;
    this.ts = ts;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public Long getTimeStamp() {
    return ts;
  }

}
