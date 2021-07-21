package com.basic.core.Component;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static com.basic.core.Utils.CastUtils.getLong;
import static org.slf4j.LoggerFactory.getLogger;

import clojure.lang.IFn.L;
import com.basic.core.Utils.FileWriter;
import com.basic.core.Utils.StopWatch;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

public class PostProcessBolt extends BaseBasicBolt {

  private static final Logger LOG = getLogger(PostProcessBolt.class);

  private static final List<String> SCHEMA = ImmutableList.of("count", "sum", "min", "max");

  private long currentTime;
  private long boltsNum;
  private StopWatch stopwatch;
  private Map<Integer, Queue<Values>> statistics;

  private int sec, tid, numTuplePSec;
  private FileWriter output;

  public PostProcessBolt(int boltsNum) {
    this.boltsNum = boltsNum;
    statistics = new HashMap<>();
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    stopwatch = StopWatch.createStarted();
    currentTime = stopwatch.elapsed(MICROSECONDS);
    sec = 1;
    tid = context.getThisTaskId();
    String prefix = "sss"  + tid;
    output = new FileWriter("/home/shuiyinyu/join/tmpresult", prefix, "txt");
    output.setFlushSize(10);
    output("metricbolt!!!!");
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Values values = new Values(tuple.getLongByField("currentMoment"), tuple.getLongByField("tuples"),
                               tuple.getLongByField("joinTimes"), tuple.getLongByField("processingDuration"),
                               tuple.getDoubleByField("latency"), tuple.getLongByField("resultNum"),
                               tuple.getLongByField("numTuplePSec"), tuple.getLongByField("numIR_hub"),
                               tuple.getLongByField("numMatch"));

    int taskId = tuple.getSourceTask();
    if (!statistics.containsKey(taskId)) {
      statistics.put(taskId, new LinkedList<>());
    }
    statistics.get(taskId).offer(values);
    if (statistics.size() == boltsNum) {
      LinkedList<Integer> list = new LinkedList<>();
      long tuples = 0;
      long processingDuration = 0;
      double latency = 0;
      int num = 0, numTuplePSec = 0;
      long numFinalR = 0, numIR_hub = 0, numMatch = 0;
      StringBuffer sb = new StringBuffer();
      for (Entry<Integer, Queue<Values>> entry : statistics.entrySet()) {
        Values temp = entry.getValue().poll();
        tuples += getLong(temp.get(1));
        processingDuration += getLong(temp.get(3));
        latency += ((Double)temp.get(4)).doubleValue();
        numTuplePSec += getLong(temp.get(6));
        num++;
        numFinalR += getLong(temp.get(5));
        numIR_hub += getLong(temp.get(7));
        numMatch += getLong(temp.get(8));

        if (entry.getValue().isEmpty()) {
          list.add(entry.getKey());
        }
      }
      for (Integer key : list) {
        statistics.remove(key);
      }

      output("@[" + sec + " sec], " + num + ": ");
      sec += 1;
      double throughput = numTuplePSec;
      if (processingDuration == 0) {
        throughput = 0;
      } else {
        throughput = throughput / processingDuration * num * 1000;
      }
      output(" "+ numFinalR);
      output(" " + numTuplePSec);
      output(" " + processingDuration);
      output(" " + num);
      output(" " + numIR_hub);
      output(" " + numMatch);
      output(" " + throughput);

      if (tuples == 0) {
        latency = 0;
      } else {
        latency /= tuples;
      }
      output(" "+latency);
      output("\n");
      LOG.info(sb.toString());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(SCHEMA));
  }

  private void output(String msg) {
    if (output != null)
      output.write(msg);
  }
}
