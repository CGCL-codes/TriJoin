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
  private static final List<String> METRIC_SCHEMA = ImmutableList.of("currentMoment","tuples", "joinTimes",
    "processingDuration", "latency", "resultNum", "numTuplePSec", "numIR_hub", "numMatch");
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
  private double latencyout;
  private Date latencyoutD;

  private long gapThrough;
  private long countPSec, numTuplePSec;
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

  private Multimap<Object, Values> currMap;
  private Multimap<Object, Values> currMapS;
  private Multimap<Object, Values> currMapIRRS;
  private Multimap<Object, Values> currMapIRST;
  private Queue<Pair> indexQueue; //RT in a queue
  private Queue<Pair> indexQueueS;
  private Queue<Pair> indexQueueIRRS;
  private Queue<Pair> indexQueueIRST;

  private FileWriter output;
  private int tid, numDispatcher, seqDAi;
  private long tst;
  private long seqDisA[][];
  private long numOutLatency;


  public JoinHashBolt(String relation_main, String relation1, boolean be, long bp, int numDisp) {
    super();
    taskRelation = relation_main;
    relationOne = relation1;

    barrier = 0l;
    barrierEnable = be;
    barrierPeriod = bp;
    numDispatcher = numDisp;
    seqDAi = 100;
    seqDisA  = new long[100][8];

    countPSec = 0;
    countRPSec = 0;
    countSPSec = 0;
    countTPSec = 0;
    gapThrough = 0;
    numOutLatency = 0;

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
    output = new FileWriter("/apache-storm-1.2.3/tmpresult/TriJoin-HTP/", prefix, "txt");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

    if (begin) {
      lastOutputTime = stopWatch.elapsed(TimeUnit.MILLISECONDS);
      begin = false;
    }

    String relR = tuple.getStringByField("relation");
    if(relR.equals("R")||relR.equals("S")||relR.equals("T")){
      countPSec++;
    }
    if(relR.equals("R"))countRPSec++;
    else if(relR.equals("S"))countSPSec++;
    else if(relR.equals("T"))countTPSec++;
    Date date = new Date();
    long currentTime = date.getTime();
    executeTuple(tuple, basicOutputCollector);

    if (isTimeToOutputProfile()) {
      output("The count of R per Second:" + countRPSec);
      output("The count of S per Second:" + countSPSec);
      output("The count of T per Second:" + countTPSec);
      countRPSec = 0;
      countSPSec = 0;
      countTPSec = 0;

      long moment = stopWatch.elapsed(TimeUnit.SECONDS);
      long tuples = numTuplesStored + numTuplesJoined - numLastProcessed;
      long joinTimes = joinedTime - lastJoinedTime;
      long processingDuration = stopWatch.elapsed(TimeUnit.MILLISECONDS) - lastOutputTime;
      long numResults = numJoinedResults - numLastJoinedResults;
      long  numIR_hub = 0, numMatch = 0;
      basicOutputCollector.emit(METRIC_STREAM_ID, new Values(moment, numOutLatency, joinTimes, processingDuration, latencyout,
              numResults, countPSec, numIR_hub, numMatch));
      output(",numOutLatency="+numOutLatency+",average latency="+latencyout/numOutLatency);
      numLastProcessed = numTuplesStored + numTuplesJoined;
      lastOutputTime = stopWatch.elapsed(TimeUnit.MILLISECONDS);
      numJoinedResults = numLastJoinedResults;
      latencyout = 0;
      numOutLatency = 0;
      countPSec = 0;
    }

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
    outputFieldsDeclarer.declareStream(RS_RESULTSTREAM_ID, new Fields("relation", "timestamp", "seq", "key", "key2", "value"));
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
      currMapIRRS.put(key, values);
      numInterResultStoredOne++;
      if (currMapIRRS.size() >= subIndexSize){
        indexQueueIRRS.offer(MutablePair.of(ts, currMapIRRS));
        currMapIRRS = LinkedListMultimap.create(subIndexSize);
      }
    }

  }

  public void store(Tuple tuple, String IRrel, int numFollower, String interresultS, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    Long seq = tuple.getLongByField("seq");
    String key = tuple.getStringByField("key");
    String key2 = tuple.getStringByField("key2");
    String value = tuple.getStringByField("value");

      String[] values = interresultS.split(",");
      String mulTupleA[];
      mulTupleA = new String[6*numFollower];
      for(int i = 0; i < numFollower*6; i += 6){
        mulTupleA[i] = values[i];
        mulTupleA[i + 1] = values[i + 1];
        mulTupleA[i + 2] = values[i + 2];
        mulTupleA[i + 3] = values[i + 3];
        mulTupleA[i + 4] = values[i + 4];
        mulTupleA[i + 5] = values[i + 5];
      }

    if(taskRelation.equals("RS")){
      basicOutputCollector.emit(RS_RESULTSTREAM_ID, new Values("RS", ts, seq, key, key2, value));
    } else if ((rel.equals("S") && IRrel.equals("T")) || (rel.equals("T") && IRrel.equals("S"))){
      currMapIRST.put(key, new Values("ST", ts, seq, key, key2, value));
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

      Date dateH = new Date();
      long currentTimeF = dateH.getTime();
      latencyout += (currentTimeF - tsOpp);
      numOutLatency++;

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

      Date dateH = new Date();
      long currentTimeF = dateH.getTime();
      latencyout += (currentTimeF - tsOpp);
      numOutLatency++;

      store(tuple);
    }


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
        join(tuple, pairIndex.getRight(), false, basicOutputCollector);
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
        join(tuple, pairIndex.getRight(), false, basicOutputCollector);
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
    String key2 = tuple.getStringByField("key2");
    long seq = tuple.getLongByField("seq");
    String value = tuple.getStringByField("value");
    long ts = tuple.getLongByField("timestamp");
    boolean isMatch = false;
    int numMatchTmp = 1;
    String storedRel = " ";

    String interresultStr = rel+","+ts+","+seq+","+key+","+key2+","+value;

    if(interResultYON){////subindex is intermediate result, output the final result.
      if(rel.equals("T")){
        for(Values storedTupleP : getMatchings(subIndex,key)){
          isMatch = true;
          }
        } else {
          for (Values storedTuple : getMatchings(subIndex, key)){
            isMatch = true;
          }
        }
    } else { /////subindex isn't intermediate result, generate the intermediate result.
      for (Values storedTuple : getMatchings(subIndex, key)){
        if(rel.equals("S")){
          if(!isMatch){
            isMatch = true;
          }
          storedRel = getString(storedTuple,0);
          String valuesM = getString(storedTuple,0)+","+getLong(storedTuple,1)+"," + getLong(storedTuple,2) + "," +
                  getString(storedTuple,3)+","+getString(storedTuple,4)+","+getString(storedTuple,5);
          interresultStr =interresultStr + "," + valuesM;
          numMatchTmp++;
        }else {
          String valuesM = getString(storedTuple,0)+","+getLong(storedTuple,1)+"," + getLong(storedTuple,2) + "," +
                  getString(storedTuple,3)+","+getString(storedTuple,4)+","+getString(storedTuple,5);
          interresultStr =interresultStr + "," + valuesM;
          numMatchTmp = 2;
          store(tuple, getString(storedTuple,0), numMatchTmp, interresultStr, basicOutputCollector);
        }
      }
//      if(rel.equals("S"))
        store(tuple, storedRel, numMatchTmp, interresultStr, basicOutputCollector);
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
