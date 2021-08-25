package com.basic.core;

import static com.basic.core.Utils.Config.*;
import static com.basic.core.Utils.StormRunner.runCluster;
import static com.basic.core.Utils.StormRunner.runLocal;
import static org.slf4j.LoggerFactory.getLogger;

import com.basic.core.Component.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutConfig.ProcessingGuarantee;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

public class Topology {

  public static final String KAFKA_BROKER = "node21:9092,node104:9092,node105:9092";
  private static final Logger LOG = getLogger(Topology.class);
  private final TopologyArgs topologyArgs = new TopologyArgs(Topology.class.getName());

  public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String servers, String topic, String groupId,
    int rate) {
    ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
      (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
            new Fields("topic", "partition", "offset", "key", "value"));
    return KafkaSpoutConfig.builder(servers, new String[]{topic})
      .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      .setRetry(getRetryService())
      .setRecordTranslator(trans)
      .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
      .setProcessingGuarantee(ProcessingGuarantee.AT_MOST_ONCE)
      .build();
  }

  public static KafkaSpoutRetryService getRetryService() {
    return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
      KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE,
      KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
  }

  public static void main(String[] args) throws Exception {
    (new Topology()).run(args);

  }

  public void run(String[] args) throws Exception {
    if (!topologyArgs.processArgs(args)) {
      return;
    }
    if (topologyArgs.help) {
      return;
    }

    StormTopology topology = createTopology(topologyArgs.remoteMode);
    if (topology == null) {
      LOG.error("Topology create failed!");
      return;
    }

    Config config = configureTopology();
    if (config == null) {
      LOG.error("Configure failed!");
      return;
    }

    if (topologyArgs.remoteMode) {
      LOG.info("execution mode: remote");
      runCluster(topologyArgs.topologyName, topology, config);
    } else {
      LOG.info("execution mode: local");
      runLocal(topologyArgs.topologyName, topology, config, topologyArgs.localRuntime);
    }
  }

  public StormTopology createTopology(boolean remoteMode) {
    TopologyBuilder builder = new TopologyBuilder();

    if (remoteMode) {
      builder.setSpout(KAFKA_SPOUT_ID_R, new RandomDataSpout("R", topologyArgs.inputfile, topologyArgs.tupleRate), topologyArgs.numKafkaSpouts);
      builder.setSpout(KAFKA_SPOUT_ID_S, new RandomDataSpout("S", topologyArgs.inputfile, topologyArgs.tupleRate), topologyArgs.numKafkaSpouts);
      builder.setSpout(KAFKA_SPOUT_ID_T, new RandomDataSpout("T", topologyArgs.inputfile, topologyArgs.tupleRate), topologyArgs.numKafkaSpouts);
    } else {
      builder.setSpout(KAFKA_SPOUT_ID_R, new RandomDataSpout("R", topologyArgs.inputfile, topologyArgs.tupleRate), topologyArgs.numKafkaSpouts);
      builder.setSpout(KAFKA_SPOUT_ID_S, new RandomDataSpout("S", topologyArgs.inputfile, topologyArgs.tupleRate), topologyArgs.numKafkaSpouts);
      builder.setSpout(KAFKA_SPOUT_ID_T, new RandomDataSpout("T", topologyArgs.inputfile, topologyArgs.tupleRate), topologyArgs.numKafkaSpouts);
    }

    builder.setBolt(SHUFFLE_BOLT_ID, new ShuffleBolt(topologyArgs.rate), topologyArgs.numShufflers)
      .shuffleGrouping(KAFKA_SPOUT_ID_R)
      .shuffleGrouping(KAFKA_SPOUT_ID_S)
      .shuffleGrouping(KAFKA_SPOUT_ID_T);

    if (topologyArgs.strategy.equals(TopologyArgs.HASH_STRATEGY)) {

      JoinHashBolt joinerRS = new JoinHashBolt("RS", "S", topologyArgs.barrierEnable,
              topologyArgs.barrierPeriod, topologyArgs.numDispatcher);
      JoinHashBolt joinerT = new JoinHashBolt("T","S", topologyArgs.barrierEnable,
              topologyArgs.barrierPeriod, topologyArgs.numDispatcher);

      builder.setBolt(DISPATCHER_BOLT_ID, new DispatchBolt(topologyArgs.barrierEnable,topologyArgs.barrierPeriod), topologyArgs.numDispatcher)
        .shuffleGrouping(SHUFFLE_BOLT_ID);

      builder.setBolt(JOINER_RS_BOLT_ID, joinerRS, topologyArgs.numPartitionsR)
        .fieldsGrouping(DISPATCHER_BOLT_ID, SHUFFLE_R_STREAM_ID, new Fields("key"))
        .fieldsGrouping(DISPATCHER_BOLT_ID, SHUFFLE_S_STREAM_ID, new Fields("key"))
        .allGrouping(DISPATCHER_BOLT_ID, TIMESTAMP_SEQ_ID);
      builder.setBolt(JOINER_T_BOLT_ID, joinerT, topologyArgs.numPartitionsT)
        .fieldsGrouping(DISPATCHER_BOLT_ID, SHUFFLE_T_STREAM_ID, new Fields("key2"))
        .fieldsGrouping(JOINER_RS_BOLT_ID, RS_RESULTSTREAM_ID, new Fields("key2"))
        .fieldsGrouping(DISPATCHER_BOLT_ID, SHUFFLE_S_STREAM_ID, new Fields("key2"))
        .allGrouping(DISPATCHER_BOLT_ID, TIMESTAMP_SEQ_ID);

      builder.setBolt(METRIC_BOLT_ID, new PostProcessBolt(topologyArgs.numPartitionsT), 1)
              .shuffleGrouping(JOINER_T_BOLT_ID, METRIC_STREAM_ID);

    } else if (topologyArgs.strategy.equals(TopologyArgs.RANDOM_STRATEGY)) {

      JoinBolt joinerR = new JoinBolt("R", "S", "T", topologyArgs.barrierEnable,
              topologyArgs.barrierPeriod, topologyArgs.numDispatcher, topologyArgs.joincondition);
      JoinBolt joinerS = new JoinBolt("S", "T", "R", topologyArgs.barrierEnable,
              topologyArgs.barrierPeriod, topologyArgs.numDispatcher, topologyArgs.joincondition);
      JoinBolt joinerT = new JoinBolt("T", "R", "S", topologyArgs.barrierEnable,
              topologyArgs.barrierPeriod, topologyArgs.numDispatcher, topologyArgs.joincondition);

      builder.setBolt(DISPATCHER_BOLT_ID, new DispatchBolt(topologyArgs.barrierEnable,topologyArgs.barrierPeriod), topologyArgs.numDispatcher)
        .shuffleGrouping(SHUFFLE_BOLT_ID);

      if(topologyArgs.joincondition.equals("3JCondition")){
        builder.setBolt(JOINER_R_BOLT_ID, joinerR, topologyArgs.numPartitionsR)
                .shuffleGrouping(DISPATCHER_BOLT_ID, SHUFFLE_R_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_S_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_T_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, TIMESTAMP_SEQ_ID);
        builder.setBolt(JOINER_S_BOLT_ID, joinerS, topologyArgs.numPartitionsS)
                .shuffleGrouping(DISPATCHER_BOLT_ID, SHUFFLE_S_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_R_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_T_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, TIMESTAMP_SEQ_ID);
        builder.setBolt(JOINER_T_BOLT_ID, joinerT, topologyArgs.numPartitionsT)
                .shuffleGrouping(DISPATCHER_BOLT_ID, SHUFFLE_T_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_R_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_S_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, TIMESTAMP_SEQ_ID);
      }else {
        builder.setBolt(JOINER_R_BOLT_ID, joinerR, topologyArgs.numPartitionsR)
                .shuffleGrouping(DISPATCHER_BOLT_ID, SHUFFLE_R_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_S_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_T_STREAM_ID)
                .allGrouping(JOINER_T_BOLT_ID, ST_RESULTSTREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, TIMESTAMP_SEQ_ID);
        builder.setBolt(JOINER_S_BOLT_ID, joinerS, topologyArgs.numPartitionsS)
                .shuffleGrouping(DISPATCHER_BOLT_ID, SHUFFLE_S_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_R_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_T_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, TIMESTAMP_SEQ_ID);
        builder.setBolt(JOINER_T_BOLT_ID, joinerT, topologyArgs.numPartitionsT)
                .shuffleGrouping(DISPATCHER_BOLT_ID, SHUFFLE_T_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_R_STREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, BROADCAST_S_STREAM_ID)
                .allGrouping(JOINER_R_BOLT_ID, SR_RESULTSTREAM_ID)
                .allGrouping(DISPATCHER_BOLT_ID, TIMESTAMP_SEQ_ID);
      }



      builder.setBolt(METRIC_BOLT_ID, new PostProcessBolt(topologyArgs.numPartitionsR + topologyArgs.numPartitionsS + topologyArgs.numPartitionsT), 1)
        .shuffleGrouping(JOINER_R_BOLT_ID, METRIC_STREAM_ID)
        .shuffleGrouping(JOINER_S_BOLT_ID, METRIC_STREAM_ID)
        .shuffleGrouping(JOINER_T_BOLT_ID, METRIC_STREAM_ID);

    }

    return builder.createTopology();
  }

  public Config configureTopology() {
    Config conf = new Config();

    conf.setNumWorkers(topologyArgs.numWorkers);
    conf.put(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING, true);

    conf.put("SUB_INDEX_SIZE", topologyArgs.subindexSize);
    conf.put("WINDOW_ENABLE", topologyArgs.window);
    conf.put("WINDOW_LENGTH", topologyArgs.windowLength);
    return conf;

  }
}
