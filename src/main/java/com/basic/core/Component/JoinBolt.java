package com.basic.core.Component;

import static com.basic.core.Utils.CastUtils.getBoolean;
import static com.basic.core.Utils.Config.*;
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

public class JoinBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(JoinBolt.class);
  private static final List<String> METRIC_SCHEMA = ImmutableList.of("currentMoment", "tuples", "joinTimes",
    "processingDuration", "latency", "resultNum", "numTuplePSec", "numIR_hub", "numMatch");
  private final String taskRelation;
  private final String relationOne;
  private final String relationTwo;

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
  private long gapThrough;
  private long countPSec;
  private int numTuplePSec;
  private int countRPSec;
  private int countSPSec;
  private int countTPSec;

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

  private Queue<Pair> indexQueue;
  private Multimap<Object, Values> currMap;
  private Multimap<Object, String[]> currMapIR1;
  private Multimap<Object, String[]> currMapIR2;
  private Queue<Pair> indexQueueIR1;
  private Queue<Pair> indexQueueIR2;
  private Queue<Integer> numMatchHubQ1,numMatchHubQ2;
  private FileWriter output;
  private int tid, numDispatcher, seqDAi;
  private long tst;
  private long seqDisA[][]; //
  private int numIR_hub1,numIR_hub2, numMatch1, numMatch2;

  private long numOutLatency, numResults;
  private int numfinalR;
  private long numIR, seqjOut;
  private String JoinCondition;

  public JoinBolt(String relation_main, String relation1, String relation2, boolean be, long bp, int numDisp, String jcond) {
    super();
    taskRelation = relation_main;
    relationOne = relation1;
    relationTwo = relation2;
    JoinCondition = jcond;

    barrier = 0l;
    barrierEnable = be;
    barrierPeriod = bp;
    tst = 0;
    numDispatcher = numDisp;
    seqDAi = 100;
    seqDisA  = new long[seqDAi][numDispatcher];///
    if (!taskRelation.equals("R") && !taskRelation.equals("S") && !taskRelation.equals("T")) {
      LOG.error("Unknown relation: " + taskRelation);
    }
    countPSec = 0;
    countRPSec = 0;
    countSPSec = 0;
    countTPSec = 0;
    gapThrough = 0;
    numIR_hub1 = 0;
    numIR_hub2 = 0;
    numMatch1 = 0;
    numMatch2 = 0;
    numOutLatency = 0;
    numResults = 0;
    latency = 0;
    numIR = 0;
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
    indexQueueIR1 = newLinkedList();
    indexQueueIR2 = newLinkedList();
    currMap = LinkedListMultimap.create(subIndexSize);
    currMapIR1 = LinkedListMultimap.create(subIndexSize);
    currMapIR2 = LinkedListMultimap.create(subIndexSize);
    numMatchHubQ1 = new LinkedList<Integer>();
    numMatchHubQ2 = new LinkedList<Integer>();

    tid = context.getThisTaskId();
    String prefix = "srj_joiner_" + taskRelation.toLowerCase() + tid;
    output = new FileWriter("/apache-storm-1.2.3/tmpResult/TriJoin-R-TP-3500perS/", prefix, "txt");

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
    } else {
      String rel = tuple.getStringByField("relation");
      long ts = tuple.getLongByField("timestamp");
      if(rel.equals("TimeStamp")){
        long seqDisT = tuple.getLongByField("seq");
        ///Comparing the "seq", all the seq is come?
        int seqAi = 0, seqAj = 0;
        seqAi = (int)(seqDisT%seqDAi);
        for(; seqAj < (numDispatcher-1); seqAj++){
          if(seqDisA[seqAi][seqAj] != seqDisT){
            seqDisA[seqAi][seqAj] = seqDisT;
            break;
          }
        }
        if(seqAj == (numDispatcher-1)){
          tst = ts;
          executeBufferedTuples(tst, basicOutputCollector);
        }
        seqjOut = seqAj;
      } else{///the generate tuple
        bufferedTuples.offer(new SortedTuple(tuple, currentTime));
      }
    }

    String relR = tuple.getStringByField("relation");
    if(relR.equals("R")||relR.equals("S")||relR.equals("T")){
      countPSec++;
    }

    Date date = new Date();
    long currentTime1 = date.getTime();
    if(currentTime1 - gapThrough >= 1000) {
      output("The count of R per Second:" + countRPSec);
      output("\n");
      output("The count of S per Second:" + countSPSec);
      output("\n");
      output("The count of T per Second:" + countTPSec);
      output("\n");
      countRPSec = 0;
      countSPSec = 0;
      countTPSec = 0;
      gapThrough = currentTime1;
      output("Time:" + stopWatch.elapsed(TimeUnit.MILLISECONDS) + "The number of hub1:"+numIR_hub1+", the number of tuple be matched 1:"+numMatch1+
              ",the number of hub2:"+numIR_hub2+", the number of tuple be matched 2:"+numMatch2);
      output("\n");
      output("indexQueue.size()="+indexQueue.size()+",indexQueueIR1.size()="+indexQueueIR1.size()+",indexQueueIR2.size()="+indexQueueIR2.size());
      output("\n");
      output("currMap.size()="+currMap.size()+",currMapIR1.size()="+currMapIR1.size()+",currMapIR2.size()="+currMapIR2.size());
      output("\n");
      output("numfinalR="+numfinalR);
      output("\n");
    }


    if (isTimeToOutputProfile()) {

      long moment = stopWatch.elapsed(TimeUnit.SECONDS);
      long tuples = numTuplesStored + numTuplesJoined - numLastProcessed;
      long joinTimes = joinedTime - lastJoinedTime;
      long processingDuration = stopWatch.elapsed(TimeUnit.MILLISECONDS) - lastOutputTime;

      long numIR_hub = numIR_hub1 + numIR_hub2;
      long numMatch = numMatch1 + numMatch2;
      output("numOutLatency="+numOutLatency+",average latency="+latency/numOutLatency+",numResults="+numResults);
      output("--------seqjOut="+seqjOut+",numDispatcher="+numDispatcher);
      output("++++++numIR_hub="+numIR_hub+",numDispatcher="+numMatch);
      basicOutputCollector.emit(METRIC_STREAM_ID, new Values(moment, numOutLatency, joinTimes, processingDuration, latency,
                numResults, countPSec, numIR_hub, numMatch));
      numLastProcessed = numTuplesStored + numTuplesJoined;
      lastJoinedTime = joinedTime;
      lastOutputTime = stopWatch.elapsed(TimeUnit.MILLISECONDS);
      numJoinedResults = numLastJoinedResults;
      latency = 0; numOutLatency = 0; countPSec = 0;
      numIR_hub1 = 0; numMatch1 = 0; numIR_hub2 = 0; numMatch2 = 0;
    }
  }

  public void executeTuple(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    if (rel.equals(taskRelation)) {
      store(tuple);
      numTuplesStored++;
    } else {
      join(tuple, basicOutputCollector);
      numTuplesJoined++;
    }
    if(rel.equals("R"))countRPSec++;
    else if(rel.equals("S"))countSPSec++;
    else if(rel.equals("T"))countTPSec++;
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

  public void executeBufferedTuples(Long tst, BasicOutputCollector basicOutputCollector) {
    while (!bufferedTuples.isEmpty()) {
      SortedTuple tempTuple = bufferedTuples.peek();
      if (tempTuple.getTuple().getLongByField("timestamp") <= tst) {
        executeTuple(tempTuple.getTuple(), basicOutputCollector);
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
    outputFieldsDeclarer.declareStream(SR_RESULTSTREAM_ID, new Fields("relation", "timestamp", "seq", "key", "key2", "value"));
    outputFieldsDeclarer.declareStream(ST_RESULTSTREAM_ID, new Fields("relation", "timestamp", "seq", "key", "key2", "value"));
  }

  public void store(Tuple tuple) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    Long seq = tuple.getLongByField("seq");
    String key = tuple.getStringByField("key");
    String key2 = tuple.getStringByField("key2");
    String value = tuple.getStringByField("value");

    Values values = new Values(rel, ts, seq, key, key2, value);
    currMap.put(key, values);

    if (currMap.size() >= subIndexSize) {
      indexQueue.offer(ImmutablePair.of(ts, currMap));
      currMap = LinkedListMultimap.create(subIndexSize);
    }
  }

  public void store(Tuple tuple, int numFollower, String interresultS) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    String key = tuple.getStringByField("key");
    String[] values = interresultS.split(",");
//    int i = 0;
    String mulTupleA[];
    mulTupleA = new String[6*numFollower];
    for(int i = 0; i < numFollower*6; i += 6) {
      mulTupleA[i] = values[i];
      mulTupleA[i + 1] = values[i + 1];
      mulTupleA[i + 2] = values[i + 2];
      mulTupleA[i + 3] = values[i + 3];
      mulTupleA[i + 4] = values[i + 4];
      mulTupleA[i + 5] = values[i + 5];
    }

    if(JoinCondition.equals("3JCondition") || (JoinCondition.equals("2JCondition") && taskRelation.equals("S"))){
      if(rel.equals(relationOne)){
        currMapIR1.put(key, mulTupleA);
        numInterResultStoredOne++;
        if (currMapIR1.size() >= subIndexSize){
          indexQueueIR1.offer(ImmutablePair.of(ts, currMapIR1));
          currMapIR1 = LinkedListMultimap.create(subIndexSize);
        }
      } else if(rel.equals(relationTwo)){
        currMapIR2.put(key, mulTupleA);
        numInterResultStoredTwo++;
        if (currMapIR2.size() >= subIndexSize){
          indexQueueIR2.offer(ImmutablePair.of(ts, currMapIR2));
          currMapIR2 = LinkedListMultimap.create(subIndexSize);
        }
      }
    }else {
      currMapIR1.put(key, mulTupleA);
      numInterResultStoredOne++;
      if (currMapIR1.size() >= subIndexSize){
        indexQueueIR1.offer(ImmutablePair.of(ts, currMapIR1));
        currMapIR1 = LinkedListMultimap.create(subIndexSize);
      }
    }

  }

  public void join(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    long tsOpp = tuple.getLongByField("timestamp");
    int numToDelete = 0, numToDelete1 = 0, numToDelete2 = 0;
    String rel = tuple.getStringByField("relation");
    String key = tuple.getStringByField("key");
    boolean isInterResult = true;

    ///relation is relationone，need to join the intermediate result that relationtwo's queue and map join，and store IR of the tuple from taskrelation.
    if(JoinCondition.equals("3JCondition") || (JoinCondition.equals("2JCondition") && taskRelation.equals("S"))) {
      if (rel.equals(relationOne)) {
        for (Pair pairIRIndex2 : indexQueueIR2) {
          long ts = getLong(pairIRIndex2.getLeft());
          if (isWindowJoin && !isInWindow(tsOpp, ts)) {
            ++numToDelete2;
            continue;
          }
          join(tuple, pairIRIndex2.getRight(), isInterResult, basicOutputCollector);
        }
        join(tuple, currMapIR2, isInterResult, basicOutputCollector);
      } else if (rel.equals(relationTwo)) {
        for (Pair pairIRIndex1 : indexQueueIR1) {
          long ts = getLong(pairIRIndex1.getLeft());
          if (isWindowJoin && !isInWindow(tsOpp, ts)) {
            ++numToDelete1;
            continue;
          }
          join(tuple, pairIRIndex1.getRight(), isInterResult, basicOutputCollector);
        }
        join(tuple, currMapIR1, isInterResult, basicOutputCollector);
      }

      numOutLatency++;
      Date date = new Date();
      long currentTimeF = date.getTime();
      latency += (currentTimeF - tsOpp);

      for (int i = 0; i < numToDelete1; ++i) {
        indexQueueIR1.poll();
        output("indexQueueIR1.poll()-----------------");
        output("\n");
      }
      for (int i = 0; i < numToDelete2; ++i) {
        indexQueueIR2.poll();
        output("indexQueueIR2.poll()-----------------");
        output("\n");
      }
      ///generate the intermediate result.
      isInterResult = false;
      for (Pair pairIndex : indexQueue) {
        long ts = getLong(pairIndex.getLeft());
        if (isWindowJoin && !isInWindow(tsOpp, ts)) {
          ++numToDelete;
          continue;
        }
        join(tuple, pairIndex.getRight(), isInterResult, basicOutputCollector);
      }
      join(tuple, currMap, isInterResult, basicOutputCollector);

      for (int i = 0; i < numToDelete; ++i) {
        indexQueue.poll();
        output("indexQueue.poll()-----------------");
        output("\n");
      }
    }else if(rel.equals("S")){
      for (Pair pairIndex : indexQueue){
        long ts = getLong(pairIndex.getLeft());
        if (isWindowJoin && !isInWindow(tsOpp, ts)) {
          ++numToDelete;
          continue;
        }
        isInterResult = false;
        join(tuple, pairIndex.getRight(), isInterResult, basicOutputCollector);
      }
      join(tuple, currMap, isInterResult, basicOutputCollector);
    }else{ ///2joincondition
      if(rel.equals("T")||rel.equals("R")){
        isInterResult = true;
        for (Pair pairIRIndex1 : indexQueueIR1) {
          long ts = getLong(pairIRIndex1.getLeft());
          if (isWindowJoin && !isInWindow(tsOpp, ts)) {
            ++numToDelete1;
            continue;
          }
          join(tuple, pairIRIndex1.getRight(), isInterResult, basicOutputCollector);
        }
        join(tuple, currMapIR1, isInterResult, basicOutputCollector);

        for (int i = 0; i < numToDelete1; ++i) {
          indexQueueIR1.poll();
          output("indexQueueIR1.poll()-----------------");
          output("\n");
        }

      }else if(rel.equals("ST")||rel.equals("SR")){
        isInterResult = true;
        for (Pair pairIndex : indexQueue) {
          long ts = getLong(pairIndex.getLeft());
          if (isWindowJoin && !isInWindow(tsOpp, ts)) {
            ++numToDelete1;
            continue;
          }
          join(tuple, pairIndex.getRight(), isInterResult, basicOutputCollector);
        }
        join(tuple, currMap, isInterResult, basicOutputCollector);

        numOutLatency++;
        Date date = new Date();
        long currentTimeF = date.getTime();
        latency += (currentTimeF - tsOpp);
      }
    }

  }

  ////
  public void join(Tuple tuple, Object subIndex, boolean isInterResult, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    long ts = tuple.getLongByField("timestamp");
    long seq = tuple.getLongByField("seq");
    String key = tuple.getStringByField("key");
    String key2 = tuple.getStringByField("key2");
    String value = tuple.getStringByField("value");
    boolean isMatch = false;
    int numMatchTmp1 = 0, numMatchTmp2 = 0, numMatchTmp = 1;

    String interresultStr = rel+","+ts+","+seq+","+key+","+key2+","+value;

    if(isInterResult){
      if(rel.equals("ST") || rel.equals("SR")){
        for (Map.Entry storedTupleP : getEntries(subIndex)){
          Values storedTuple = (Values) storedTupleP.getValue();
          Double diff = Double.parseDouble(getString(storedTuple,3))-Double.parseDouble(key);
          if(Math.abs(diff) < 0.0001){
            numResults++;
          }
        }
      }else {
        for (Map.Entry storedTupleP : getArrEntries(subIndex)){
          String storedKey = (String) storedTupleP.getKey();
          Double diff = Double.parseDouble(storedKey)-Double.parseDouble(key);
          if(Math.abs(diff) < 0.0001){
            numResults++;
          }
        }
      }

    }else {
      for (Map.Entry storedTupleP : getEntries(subIndex)){
        Values storedTuple = (Values) storedTupleP.getValue();
        Double diff = Double.parseDouble(getString(storedTuple,3))-Double.parseDouble(key);

        if(Math.abs(diff) < 0.00001){
          if(!isMatch){
            isMatch = true;
            if(rel.equals(relationOne))
              numIR_hub2++;
            else if(rel.equals(relationTwo))numIR_hub1++;
          }
          if(rel.equals(relationOne)){
            numMatch2++;
          }
          else if(rel.equals(relationTwo)){
            numMatch1++;
          }

          String values = getString(storedTuple,0)+","+getLong(storedTuple,1)+","+getLong(storedTuple,2)+","+
                  getString(storedTuple,3)+","+getString(storedTuple,4)+","+getString(storedTuple,5);
          interresultStr = interresultStr + "," + values;
          numMatchTmp++;
          numIR++;
        }
      }
      if(isMatch) {
        store(tuple, numMatchTmp, interresultStr);
        if(JoinCondition.equals("2JCondition")){
          if(taskRelation.equals("R"))basicOutputCollector.emit(SR_RESULTSTREAM_ID, new Values("SR", ts, seq, key, key2, value));
          if(taskRelation.equals("T"))basicOutputCollector.emit(ST_RESULTSTREAM_ID, new Values("ST", ts, seq, key, key2, value));
        }
      }
    }

  }

  @SuppressWarnings("unchecked")
  private Collection< Map.Entry<Object, Values> > getEntries(Object index) {
    return ((Multimap<Object, Values>) index).entries();
  }

  @SuppressWarnings("unchecked")
  private Collection< Map.Entry<Object, String[]> > getArrEntries(Object index) {
    return ((Multimap<Object, String[]>) index).entries();
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
    if(Math.abs(tsNewTuple - tsStoredTuple) <= windowLength)return true;
    else {
      return false;
    }
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
