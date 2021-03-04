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
    "processingDuration", "latency", "resultNum");
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

  private Queue<Pair> indexQueue;
  private Multimap<Object, Values> currMap;
  private Multimap<Object, Values> currMap1;
  private Multimap<Object, String> currMapIR1;
  private Multimap<Object, String> currMapIR2;
  private Queue<Pair> indexQueueIR1;
  private Queue<Pair> indexQueueIR2;

  private FileWriter output;
  private int tid, numDispatcher, seqDAi;
  private long tst;
  private long seqDisA[][]; //
//  private int[] seqA_h = new int[8];

  public JoinBolt(String relation_main, String relation1, String relation2, boolean be, long bp, int numDisp) {
    super();
    taskRelation = relation_main;
    relationOne = relation1;
    relationTwo = relation2;

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

    tid = context.getThisTaskId();
    String prefix = "srj_joiner_" + taskRelation.toLowerCase() + tid;
    output = new FileWriter("/yushuiy/apache-storm-1.2.3/tmpresult-OrS/", prefix, "txt");
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
      latency += (stopWatch.elapsed(TimeUnit.MICROSECONDS) - currentTime) / 1000;
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
      } else{///the generate tuple
        bufferedTuples.offer(new SortedTuple(tuple, currentTime));
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
  }

  public void executeTuple(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    if (rel.equals(taskRelation) && (ts < tst)) {
      store(tuple);
      numTuplesStored++;
    } else {
      join(tuple, basicOutputCollector);
      numTuplesJoined++;
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
  }

  public void store(Tuple tuple) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    String key = tuple.getStringByField("key");
    String value = tuple.getStringByField("value");

    Values values = new Values(rel, ts, key, value);
    currMap.put(key, values);

    if (currMap.size() >= subIndexSize) {
      indexQueue.offer(ImmutablePair.of(ts, currMap));
      currMap = LinkedListMultimap.create(subIndexSize);
    }
  }

  public void store(Tuple tuple, String IRrel, String interresultS) {
    String rel = tuple.getStringByField("relation");
    Long ts = tuple.getLongByField("timestamp");
    String key = tuple.getStringByField("key");
    String value = tuple.getStringByField("value");

    String valuess = rel+","+ts+","+key+","+value;
    valuess += interresultS;

    ///currMapIR1 store the intermediate result about relationone，currMapIR2 store the intermediate result about relationtwo.
    if(IRrel.equals(relationOne)){
      currMapIR1.put(key, valuess);
      numInterResultStoredOne++;
      if (currMapIR1.size() >= subIndexSize){
        indexQueueIR1.offer(ImmutablePair.of(ts, currMapIR1));
        currMapIR1 = LinkedListMultimap.create(subIndexSize);
      }
    } else if(IRrel.equals(relationTwo)){
      currMapIR2.put(key, valuess);numInterResultStoredTwo++;
      if (currMapIR2.size() >= subIndexSize){
        indexQueueIR2.offer(ImmutablePair.of(ts, currMapIR2));
        currMapIR2 = LinkedListMultimap.create(subIndexSize);
      }
    }
  }

  public void join(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    long tsOpp = tuple.getLongByField("timestamp");
    int numToDelete = 0, numToDelete1 = 0, numToDelete2 = 0;
    String rel = tuple.getStringByField("relation");
    String key = tuple.getStringByField("key");
    boolean interResultYON = true;

    ///relation is relationone，need to join the intermediate result that relationtwo's queue and map join，and store IR of the tuple from taskrelation.
    if(rel.equals(relationOne)){
      for(Pair pairIRIndex2 : indexQueueIR2){
        long ts = getLong(pairIRIndex2.getLeft());
        if (isWindowJoin && !isInWindow(tsOpp, ts)){
          ++numToDelete2;
          continue;
        }
        join(tuple, pairIRIndex2.getRight(), interResultYON, basicOutputCollector);
      }
      join(tuple, currMapIR2, interResultYON, basicOutputCollector);
    }else if(rel.equals(relationTwo)){
      for(Pair pairIRIndex1 : indexQueueIR1){
        long ts = getLong(pairIRIndex1.getLeft());
        if (isWindowJoin && !isInWindow(tsOpp, ts)){
          ++numToDelete1;
          continue;
        }
        join(tuple, pairIRIndex1.getRight(), interResultYON, basicOutputCollector);
      }
      join(tuple, currMapIR1, interResultYON, basicOutputCollector);
    }
    Date date = new Date();
    long currentTimeF = date.getTime();
    latencyout = (currentTimeF - tsOpp);
    latencyoutD = new Date(latencyout);
    output("Matched!! The complete time = " + currentTimeF+","+"tsOpp="+tsOpp+",latencyout="+latencyout);

    for (int i = 0; i < numToDelete1; ++i) {
      indexQueueIR1.poll();
    }
    for (int i = 0; i < numToDelete2; ++i) {
      indexQueueIR2.poll();
    }
    ///generate the intermediate result.
    for(Pair pairIndex : indexQueue){
      long ts = getLong(pairIndex.getLeft());
      if (isWindowJoin && !isInWindow(tsOpp, ts)){
        ++numToDelete;
        continue;
      }
      interResultYON = false;
      join(tuple, pairIndex.getRight(), interResultYON, basicOutputCollector); ///看看这个存储是不是这样滴,这个join函数也不一样了。。。
    }
    interResultYON = false;
    join(tuple, currMap, interResultYON, basicOutputCollector);
  }

  ////
  public void join(Tuple tuple, Object subIndex, boolean interResultYON, BasicOutputCollector basicOutputCollector) {
    String rel = tuple.getStringByField("relation");
    String key = tuple.getStringByField("key");
    String value = tuple.getStringByField("value");

    long ts = tuple.getLongByField("timestamp");
    int numinterG = 0;
    boolean IRMatchedYoN = false;
    String interresultStr = ",";

      for (Map.Entry storedTupleP : getEntries(subIndex)){
        Values storedTuple = (Values) storedTupleP.getValue();
        if(interResultYON){////subindex is intermediate result, generate the final result.
          output(relationOne+":" + value + "---- " + tuple.getStringByField("value") );
        }else{ ////subindex is the tuple that one stream, generate the intermediate result.
          String values = getString(storedTuple,0)+","+getLong(storedTuple,1)+","+
                  getString(storedTuple,2)+","+getString(storedTuple,3);
          interresultStr += values + ",";
          IRMatchedYoN = true;
        }
        store(tuple, getString(storedTuple,0), interresultStr);
      }
      if(!IRMatchedYoN){
        Date date = new Date();
        long currentTimeF = date.getTime();
        latencyout = (currentTimeF - ts);
        latencyoutD = new Date(latencyout);
        output("Unmatched## The complete time = " + currentTimeF+","+"tsOpp="+ts+",latencyout="+latencyout);
      }
  }

  @SuppressWarnings("unchecked")
  private Collection<Values> getMatchings(Object index, Object value) {
    return ((Multimap<Object, Values>) index).get(value);
  }

  @SuppressWarnings("unchecked")
  private Collection< Map.Entry<Object, Values> > getEntries(Object index) {
    return ((Multimap<Object, Values>) index).entries();
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
