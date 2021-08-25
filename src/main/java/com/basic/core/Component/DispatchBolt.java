package com.basic.core.Component;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import static com.basic.core.Utils.Config.*;
import com.basic.core.Utils.StopWatch;
import java.util.concurrent.TimeUnit;
import java.util.Date;
import java.text.SimpleDateFormat;

import com.basic.core.Utils.FileWriter;

public class DispatchBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(DispatchBolt.class);
  private long seqDis = 0;
  private long barrierPeriod = 0;
  private boolean barrierEnable = false;
  private Date DLastBarrier = new Date();
  private long dLastBarrier = DLastBarrier.getTime();
  private String dispStrategy;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    seqDis = 0;
  }

  public DispatchBolt(boolean be, long bp){
    barrierPeriod = bp;
    barrierEnable = be;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    Date date = new Date();
    Long ts = date.getTime();
    Long seq = seqDis;
    String key = tuple.getStringByField("key");
    String key2 = tuple.getStringByField("key2");
    String value = tuple.getStringByField("value");

    Values values = new Values(rel, ts, seqDis, key, key2, value);
    if (rel.equals("R")) {
      basicOutputCollector.emit(SHUFFLE_R_STREAM_ID, values);
      basicOutputCollector.emit(BROADCAST_R_STREAM_ID, values);
    } else if (rel.equals("S")) {
      basicOutputCollector.emit(SHUFFLE_S_STREAM_ID, values);
      basicOutputCollector.emit(BROADCAST_S_STREAM_ID, values);
    } else if (rel.equals("T")){
      basicOutputCollector.emit(SHUFFLE_T_STREAM_ID, values);
      basicOutputCollector.emit(BROADCAST_T_STREAM_ID, values);
    }
    Date dateNow = new Date();
    long datenow = dateNow.getTime();
    if(((datenow-dLastBarrier) > barrierPeriod) && barrierEnable){
      seqDis = seqDis + 1;
      dLastBarrier = datenow;
      String relt = "TimeStamp";
      Values timetuple = new Values(relt, datenow, seqDis, key, key2, value);
      basicOutputCollector.emit(TIMESTAMP_SEQ_ID, timetuple);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(SHUFFLE_R_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(SHUFFLE_S_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(SHUFFLE_T_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(BROADCAST_R_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(BROADCAST_S_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(BROADCAST_T_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(TIMESTAMP_SEQ_ID, new Fields(SCHEMA));
  }

  @Override
  public void cleanup() {

  }

}
