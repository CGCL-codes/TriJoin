package com.basic.core.Component;

import java.io.*;
import java.util.Map;
import java.util.Random;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.utils.Utils;

public class RandomDataSpout extends BaseRichSpout {

  private static final Logger LOG = LoggerFactory.getLogger(RandomDataSpout.class);
  private final String rel;
  private final String topic;
//  private FileWriter output;

  private Random random;
  private SpoutOutputCollector spoutOutputCollector;

  private final int tupleRate;//1000*8
  private long tuplesPerTime;
  private BufferedReader bufferedReader;
  private final int intLower = 0;
  private final int intUpper = 200;
  private final double lngLower = 102.54;
  private final double lngUpper = 104.53;
  private final double latLower = 30.05;
  private final double latUpper = 31.26;
  private final String inputFile;

  public RandomDataSpout(String rel, String inFile, int tupleR) {
    super();
    this.rel = rel;
    inputFile = inFile;
    if(rel.equals("R"))topic="Orders2";
    else if(rel.equals("S"))topic="Gps2";
    else topic="Gps3";
    tupleRate = tupleR;
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    random = new Random();
    spoutOutputCollector = collector;
    tuplesPerTime = tupleRate/10;

    try {
      bufferedReader = new BufferedReader(new FileReader("/join/didi/" + rel + "/" + inputFile)); ///
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
  ///

  @Override
  public void nextTuple() {
    for (int i = 0; i < tuplesPerTime; i++) {
      Values values = null;
      String line = null;
      try {
        if ((line = bufferedReader.readLine()) != null) {
          values = new Values(topic, line);
          spoutOutputCollector.emit(values);
        }

      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    Utils.sleep(100);
  }


  public Values generateData() {

    String value = "0,1,2,";
    double lng = lngLower + random.nextDouble() * (lngUpper - lngLower);
    double lat = latLower + random.nextDouble() * (latUpper - latLower);
    value += String.format("%.4f,%.4f", lng, lat);
    return new Values(topic, value);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("topic", "value"));
  }
}
