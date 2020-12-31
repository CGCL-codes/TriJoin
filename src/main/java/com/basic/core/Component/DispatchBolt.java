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

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    Date date = new Date();
    Long ts = date.getTime();
    String key = tuple.getStringByField("key");
    String key2 = tuple.getStringByField("key2");
    String value = tuple.getStringByField("value");

    Values values = new Values(rel, ts, key, key2, value);
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
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(SHUFFLE_R_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(SHUFFLE_S_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(SHUFFLE_T_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(BROADCAST_R_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(BROADCAST_S_STREAM_ID, new Fields(SCHEMA));
    outputFieldsDeclarer.declareStream(BROADCAST_T_STREAM_ID, new Fields(SCHEMA));
  }

  @Override
  public void cleanup() {

  }

}
