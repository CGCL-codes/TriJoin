package com.basic.core.Component;

import static com.basic.core.Utils.CastUtils.getBoolean;
import static com.basic.core.Utils.Config.*;
import static com.basic.core.Utils.Config.RS_RESULTSTREAM_ID;
import static org.slf4j.LoggerFactory.getLogger;

import com.basic.core.Utils.StopWatch;
import com.google.common.collect.ImmutableList;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

import static com.basic.core.Utils.CastUtils.getInt;
import static com.basic.core.Utils.CastUtils.getLong;
import static com.basic.core.Utils.CastUtils.getString;

import java.util.Queue;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import java.util.Map;
import java.util.Collection;
import java.util.List;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import static com.google.common.collect.Lists.newLinkedList;
import com.basic.core.Utils.FileWriter;
import java.io.*;
import java.util.Scanner;
import java.util.Date;
import java.text.SimpleDateFormat;

public class JoinHashBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(JoinHashBolt.class);
  private static final List<String> METRIC_SCHEMA = ImmutableList.of("currentMoment", "tuples", "joinTimes",
    "processingDuration", "latency", "resultNum");
  private final String taskRelation;
  private final String relationOne;

  private boolean begin;
  private long numTuplesJoined;
  private long numTuplesStored;
  private long numInterResultStored;
  private long numInterResultStoredOne;
  private long numInterResultStoredTwo;
  private long numLastProcessed;
  private long numJoinedResults;
  private long numLastJoinedResults;
  private long joinedTime;
  private long lastJoinedTime;
  private long lastOutputTime;
  private double latency;
  private long latencyout;
  private Date latencyoutD;

  private int subIndexSize;

  private boolean isWindowJoin;
  private long windowLength;

  private StopWatch stopWatch;
  private long profileReportInSeconds;
  private long triggerReportInSeconds;

  private Queue<SortedTuple> bufferedTuples;
  private Long barrier;
  private boolean barrierEnable;
  private Long barrierPeriod;
  private Map<Long, Long> upstreamBarriers;
  private int numUpstreamTask;

  private Multimap<Object, Values> currMap;
  private Multimap<Object, Values> currMapS;
  private Multimap<Object, String> currMapIRRS;
  private Multimap<Object, String> currMapIRST;
  private Queue<Pair> indexQueue; //RT in a queue
  private Queue<Pair> indexQueueS;
  private Queue<Pair> indexQueueIRRS;
  private Queue<Pair> indexQueueIRST;

  private FileWriter output;
  private int tid, numDispatcher, seqDAi;
  private long tst;
  private long seqDisA[][];


  public JoinHashBolt(String relation_main, String relation1, boolean be, long bp, int numDisp) {
    super();
    taskRelation = relation_main;
    relationOne = relation1;

    barrier = 0l;
    barrierEnable = be;
    barrierPeriod = bp;
    numDispatcher = numDisp;
    seqDAi = 100;
    seqDisA  = new long[seqDAi][numDispatcher];

    if (!taskRelation.equals("R") && !taskRelation.equals("S") && !taskRelation.equals("T")) {
      LOG.error("Unknown relation: " + taskRelation);
    }

  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    numTuplesJoined = 0;
    numTuplesStored = 0;
    numInterResultStored = 0;
    numInterResultStoredOne = 0;
    numInterResultStoredTwo = 0;

    subIndexSize = getInt(stormConf.get("SUB_INDEX_SIZE"));
    isWindowJoin = getBoolean(stormConf.get("WINDOW_ENABLE"));
    windowLength = getLong(stormConf.get("WINDOW_LENGTH"));

    begin = true;
    stopWatch = StopWatch.createStarted();
    profileReportInSeconds = 1;
    triggerReportInSeconds = 1;
    bufferedTuples = new PriorityQueue<>(
      Comparator.comparing(o -> o.getTuple().getLongByField("timestamp")));
    upstreamBarriers = new HashMap<>();

    indexQueue = newLinkedList();
    indexQueueS = newLinkedList();
    indexQueueIRRS = newLinkedList();
    indexQueueIRST = newLinkedList();

    currMap = LinkedListMultimap.create(subIndexSize);
    currMapS = LinkedListMultimap.create(subIndexSize);
    currMapIRRS = LinkedListMultimap.create(subIndexSize);
    currMapIRST = LinkedListMultimap.create(subIndexSize);

    tid = context.getThisTaskId();
    String prefix = "srj_joiner_" + taskRelation.toLowerCase() + tid;
    output = new FileWriter("/yushuiy/apache-storm-1.2.3/tmpresult-HOS/", prefix, "txt");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

    if (begin) {
      lastOutputTime = stopWatch.elapsed(TimeUnit.MILLISECONDS);
      begin = false;
    }
    long currentTime = stopWatch.elapsed(TimeUnit.MICROSECONDS);

    if (!barrierEnable) {
      executeTuple(tuple, basicOutputCollector);
      System.out.println(tuple.getStringByField("relation"));
      latency += (stopWatch.elapsed(TimeUnit.MICROSECONDS) - currentTime) / 1000;
    } else {
      long ts = tuple.getLongByField("timestamp");

      if (!barrierEnable) {
        executeTuple(tuple, basicOutputCollector);
        latency += (stopWatch.elapsed(TimeUnit.MICROSECONDS) - currentTime) / 1000;
      } else {
        String rel = tuple.getStringByField("relation");
        long tst = tuple.getLongByField("timestamp");
        if(rel.equals("TimeStamp")){
          long seqDisT = tuple.getLongByField("seq");
          int seqAi = 0, seqAj = 0;
          seqAi = (int)(seqDisT%seqDAi);
          for(; seqAj < (numDispatcher-1); seqAj++){
            if(seqDisA[seqAi][seqAj] != seqDisT){
              seqDisA[seqAi][seqAj] = seqDisT;
              break;
            }
          }
          if(seqAj == (numDispatcher-1)){
            executeBufferedTuples(tst, basicOutputCollector);
          }
        } else{
          bufferedTuples.offer(new SortedTuple(tuple, currentTime));
        }
      }
    }

    if (isTimeToOutputProfile()) {
      long moment = stopWatch.elapsed(TimeUnit.SECONDS);
      long tuples = numTuplesStored + numTuplesJoined - numLastProcessed;
      long joinTimes = joinedTime - lastJoinedTime;
      long processingDuration = stopWatch.elapsed(TimeUnit.MILLISECONDS) - lastOutputTime;
      long numResults = numJoinedResults - numLastJoinedResults;
      basicOutputCollector.emit(METRIC_STREAM_ID, new Values(moment, tuples, joinTimes, processingDuration, latency,
        numResults));
      numLastProcessed = numTuplesStored + numTuplesJoined;
      lastJoinedTime = joinedTime;
      lastOutputTime = stopWatch.elapsed(TimeUnit.MILLISECONDS);
      numJoinedResults = numLastJoinedResults;
      latency = 0;
    }
    long processingDuration = stopWatch.elapsed(TimeUnit.MILLISECONDS) - lastOutputTime;
    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss",Locale.getDefault());
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+0"));
    Date dateLatency = new Date(latencyout+8*60*60*1000);
    sdf.format(dateLatency);
    output("latencyout="+latencyout+", latencyoutD= " + dateLatency.toString() + ",processingDuration= "+ processingDuration);
  }

  public void executeTuple(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
      if(rel.equals("R")||rel.equals("S")||rel.equals("T")){
        join(tuple, basicOutputCollector);/////store tuple, for R and S, emit it.
        numTuplesJoined++;
      } else{
        /// join T; join(intermediate, basicoutputcollector)，store IR tuple
        join(tuple, basicOutputCollector);
      }
  }

  private Long checkBarrier() {
    if (upstreamBarriers.size() != numUpstreamTask) {
      return barrier;
    }
    long tempBarrier = Long.MAX_VALUE;
    for (Map.Entry<Long, Long> entry : upstreamBarriers.entrySet()) {
      tempBarrier = Math.min(entry.getValue() / barrierPeriod, tempBarrier);
    }
    return tempBarrier;
  }

  public void executeBufferedTuples(Long barrier, BasicOutputCollector basicOutputCollector) {
    while (!bufferedTuples.isEmpty()) {
      SortedTuple tempTuple = bufferedTuples.peek();
      if (tempTuple.getTuple().getLongByField("timestamp") <= barrier) {
        executeTuple(tempTuple.getTuple(), basicOutputCollector);
        latency += (stopWatch.elapsed(TimeUnit.MICROSECONDS) - tempTuple.getTimeStamp()) / 1000;
        bufferedTuples.poll();
      } else {
        break;
      }
    }
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream(JOIN_RESULTS_STREAM_ID, new Fields("value", "rel"));
    outputFieldsDeclarer.declareStream(METRIC_STREAM_ID, new Fields(METRIC_SCHEMA));
    outputFieldsDeclarer.declareStream(RS_RESULTSTREAM_ID, new Fields("relation", "timestamp", "key", "key2", "value"));
  }

  public void store(Tuple tuple) {

    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    String key = tuple.getStringByField("key");
    String key2 = tuple.getStringByField("key2");
    String value = tuple.getStringByField("value");

    Values values = new Values(rel, ts, key, key2, value);
    currMap.put(key, values);

    if (rel.equals("R") || rel.equals("T")){
      currMap.put(key, values);
      numInterResultStoredOne++;
      if (currMap.size() >= subIndexSize){
        indexQueue.offer(ImmutablePair.of(ts, currMap));
        currMap = LinkedListMultimap.create(subIndexSize);
      }
    } else if (rel.equals("S")){
      currMapS.put(key, values);
      numInterResultStoredOne++;
      if (currMapS.size() >= subIndexSize){
        indexQueueS.offer(ImmutablePair.of(ts, currMapS));
        currMapS = LinkedListMultimap.create(subIndexSize);
      }
    } else if (rel.equals("RS")){
      String valuesS = rel+","+ts+","+key+","+key2+","+value;
      currMapIRRS.put(key, valuesS);
      numInterResultStoredOne++;
      if (currMapIRRS.size() >= subIndexSize){
        indexQueueIRRS.offer(MutablePair.of(ts, currMapIRRS));
        currMapIRRS = LinkedListMultimap.create(subIndexSize);
      }
    }

  }

  public void store(Tuple tuple, String IRrel, String interresultS, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    String key = tuple.getStringByField("key");
    String key2 = tuple.getStringByField("key2");
    String value = tuple.getStringByField("value");

    String valuess = rel + "," + ts + "," +  key + "," + key2 + "," + value;
    valuess += interresultS;

    if(taskRelation.equals("RS")){
      basicOutputCollector.emit(RS_RESULTSTREAM_ID, new Values("RS", ts, key, key2, valuess));
    } else if ((rel.equals("S") && IRrel.equals("T")) || (rel.equals("T") && IRrel.equals("S"))){
      currMapIRST.put(key, valuess);
      numInterResultStoredOne++;
      if (currMapIRST.size() >= subIndexSize){
        indexQueueIRST.offer(MutablePair.of(ts, currMapIRST));
        currMapIRST = LinkedListMultimap.create(subIndexSize);
      }
    }

  }

  public void join(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    long tsOpp = tuple.getLongByField("timestamp");
    int numToDeleteRS = 0, numToDeleteT = 0, numToDeleteST = 0, numToDeleteRoT = 0, numToDeleteS = 0;
    String rel = tuple.getStringByField("relation");
    String key = tuple.getStringByField("key");
    boolean interResultYON = true;

    if(rel.equals("T")){  ///join the IR
      for(Pair pairIRIndexT : indexQueueIRRS){
        long ts = getLong(pairIRIndexT.getLeft());
        if (isWindowJoin && !isInWindow(tsOpp, ts)){
          ++numToDeleteRS;
          continue;
        }
        join(tuple, pairIRIndexT.getRight(), true, basicOutputCollector);
      }
      join(tuple, currMapIRRS, true, basicOutputCollector);
      store(tuple);
    } else if(rel.equals("RS")){
      for(Pair pairIRIndex1 : indexQueueIRST){
        long ts = getLong(pairIRIndex1.getLeft());
        if (isWindowJoin && !isInWindow(tsOpp, ts)){
          ++numToDeleteT;
          continue;
        }
        join(tuple, pairIRIndex1.getRight(), true, basicOutputCollector);
      }
      join(tuple, currMapIRST, true, basicOutputCollector);
      store(tuple);
    }
    Date date = new Date();
    long currentTimeF = date.getTime();
    latencyout = (currentTimeF - tsOpp);
    latencyoutD = new Date(latencyout);
    output("Matched!!in the join(). The complete time = " + currentTimeF+","+"tsOpp="+tsOpp+",latencyout="+latencyout);

    for (int i = 0; i < numToDeleteRS; ++i) {
      indexQueueIRRS.poll();
    }
    for (int i = 0; i < numToDeleteT; ++i) {
      indexQueueIRST.poll();
    }
    ///generate intermediate result.
    interResultYON = false;
    if(rel.equals("S")) {
      for(Pair pairIndex : indexQueue){
        long ts = getLong(pairIndex.getLeft());
        if (isWindowJoin && !isInWindow(tsOpp, ts)){
          ++numToDeleteS;
          continue;
        }
        join(tuple, pairIndex.getRight(), false, basicOutputCollector); ///看看这个存储是不是这样滴,这个join函数也不一样了。。。
      }
      join(tuple, currMap, false, basicOutputCollector);
      store(tuple); ///indexQueueS,currMapS
    } else if (rel.equals("R") || rel.equals("T")){
      for(Pair pairIndex : indexQueueS){
        long ts = getLong(pairIndex.getLeft());
        if (isWindowJoin && !isInWindow(tsOpp, ts)){
          ++numToDeleteRoT;
          continue;
        }
        join(tuple, pairIndex.getRight(), false, basicOutputCollector); ///看看这个存储是不是这样滴,这个join函数也不一样了。。。
      }
      join(tuple, currMapS, false, basicOutputCollector);
      store(tuple);////currMap; indexQueue.
    }
    for (int i = 0; i < numToDeleteS; ++i) {
      indexQueue.poll();
    }
    for (int i = 0; i < numToDeleteRoT; ++i) {
      indexQueueS.poll();
    }
  }

  ////
  public void join(Tuple tuple, Object subIndex, boolean interResultYON, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    String key = tuple.getStringByField("key");
    String value = tuple.getStringByField("value");
    long ts = tuple.getLongByField("timestamp");
    boolean matchedYoN = false;
    String interresultStr = null;

    if(interResultYON){////subindex is intermediate result, output the final result.
      if(rel.equals("T")){
        for(String storedTupleP : getMatchingsS(subIndex,key)){
          ///output the final result.
          output("The output####");
          matchedYoN = true;
          }
        } else {
          for (String storedTuple : getMatchingsS(subIndex, key)){
            output("The output###"+rel+":" + value + "---- " + tuple.getStringByField("value"));
            matchedYoN = true;
          }
        }
    } else { /////subindex isn't intermediate result, generate the intermediate result.
      for (Values storedTuple : getMatchings(subIndex, key)){
        String values = getString(storedTuple,0)+","+getLong(storedTuple,1)+","+
                getString(storedTuple,2)+","+getString(storedTuple,3)+","+getString(storedTuple,4);
        interresultStr += values;
        store(tuple, getString(storedTuple,0), interresultStr, basicOutputCollector);
        matchedYoN = true;
      }
      if(!matchedYoN && (rel.equals("R") || rel.equals("S"))){
        Date date = new Date();
        long currentTimeF = date.getTime();
        latencyout = (currentTimeF - ts);
        latencyoutD = new Date(latencyout);
        output("Unmatched!! The complete time = " + currentTimeF+","+"tsOpp="+ts+",Latency="+latencyout);
      }
    }

  }

  @SuppressWarnings("unchecked")
  private Collection<Values> getMatchings(Object index, Object value) {
    return ((Multimap<Object, Values>) index).get(value);
  }

  @SuppressWarnings("unchecked")
  private Collection<String> getMatchingsS(Object index, Object value) {
    return ((Multimap<Object, String>) index).get(value);
  }

  @SuppressWarnings("unchecked")
  private int getIndexSize(Object index) {
    return ((Multimap<Object, Values>) index).size();
  }

  @SuppressWarnings("unchecked")
  private int getNumTuplesInWindow() {
    int numTuples = 0;
    for (Pair pairTsIndex : indexQueue) {
      numTuples += ((Multimap<Object, Values>) pairTsIndex.getRight())
              .size();
    }
    numTuples += currMap.size();

    return numTuples;
  }

  public boolean isInWindow(long tsNewTuple, long tsStoredTuple) {
    return Math.abs(tsNewTuple - tsStoredTuple) <= windowLength;
  }

  public boolean isTimeToOutputProfile() {
    long currentTime = stopWatch.elapsed(TimeUnit.SECONDS);
    if (currentTime >= triggerReportInSeconds) {
      triggerReportInSeconds = currentTime + profileReportInSeconds;
      return true;
    } else {
      return false;
    }
  }

  private void output(String msg) {
    if (output != null)
        output.write(msg);
  }

}
