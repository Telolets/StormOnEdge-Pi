import StormOnEdge.grouping.stream.ZoneShuffleGrouping;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import io.latent.storm.rabbitmq.Declarator;
import io.latent.storm.rabbitmq.UnanchoredRabbitMQSpout;
import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.Map;

/**
 * Created by ken on 12/14/2015.
 */
public class PiClusterTopology {
  private static final Log LOG = LogFactory.getLog(PiClusterTopology.class);

  @Option(name="--help", aliases={"-h"}, usage="print help message")
  private boolean _help = false;

  @Option(name="--debug", aliases={"-d"}, usage="enable debug")
  private boolean _debug = false;

  @Option(name="--globalAlert", aliases={"-g"},
          usage="Do the computation in global TaskGroup. By default system will process everything locally")
  private boolean _globalAlert = false;

  @Option(name="--numTopologies", aliases={"-n"}, metaVar="TOPOLOGIES",
          usage="number of topologies to run in parallel")
  private int _numTopologies = 1;

  @Option(name="--localTaskGroup", aliases={"--localGroup"}, metaVar="LOCALGROUP",
          usage="number of initial local TaskGroup")
  private int _localGroup = 2;

  @Option(name="--spoutParallel", aliases={"--spout"}, metaVar="SPOUT",
          usage="number of spouts to run local TaskGroup")
  private int _spoutParallel = 2;

  @Option(name="--boltParallelLocal", aliases={"--boltLocal"}, metaVar="BOLTLOCAL",
          usage="number of bolts to run local TaskGroup")
  private int _boltLocalParallel = 2;

  @Option(name="--boltParallelGlobal", aliases={"--boltGlobal"}, metaVar="BOLTGLOBAL",
          usage="number of bolts to run global TaskGroup")
  private int _boltGlobalParallel = 2;

  @Option(name="--numWorkers", aliases={"--workers"}, metaVar="WORKERS",
          usage="number of workers to use per topology")
  private int _numWorkers = 50;

  @Option(name="--ackers", metaVar="ACKERS",
          usage="number of acker bolts to launch per topology")
  private int _ackers = 0;

  @Option(name="--maxSpoutPending", aliases={"--maxPending"}, metaVar="PENDING",
          usage="maximum number of pending messages per spout (only valid if acking is enabled)")
  private int _maxSpoutPending = -1;

  @Option(name="--name", aliases={"--topologyName"}, metaVar="NAME",
          usage="base name of the topology (numbers may be appended to the end)")
  private String _name = "test";

  @Option(name="--pollFreqSec", aliases={"--pollFreq"}, metaVar="POLL",
          usage="How often should metrics be collected")
  private int _pollFreqSec = 30;

  @Option(name="--testTimeSec", aliases={"--testTime"}, metaVar="TIME",
          usage="How long should the benchmark run for.")
  private int _testRunTimeSec = 5 * 60;

  @Option(name="--sampleRateSec", aliases={"--sampleRate"}, metaVar="SAMPLE",
          usage="Sample rate for metrics (0-1).")
  private double _sampleRate = 0.3;

  private static class MetricsState {
    long transferred = 0;
    int slotsUsed = 0;
    long lastTime = 0;
  }

  private boolean printOnce = true;

  public void metrics(Nimbus.Client client, int poll, int total) throws Exception {
    System.out.println("status\ttopologies\ttotalSlots\tslotsUsed\ttotalExecutors\texecutorsWithMetrics\ttime\ttime-diff ms\ttransferred\tthroughput (MB/s)");
    MetricsState state = new MetricsState();
    long pollMs = poll * 1000;
    long now = System.currentTimeMillis();
    state.lastTime = now;
    long startTime = now;
    long cycle = 0;
    long sleepTime;
    long wakeupTime;
    while (metrics(client, now, state, "WAITING")) {
      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    }

    now = System.currentTimeMillis();
    cycle = (now - startTime)/pollMs;
    wakeupTime = startTime + (pollMs * (cycle + 1));
    sleepTime = wakeupTime - now;
    if (sleepTime > 0) {
      Thread.sleep(sleepTime);
    }
    now = System.currentTimeMillis();
    long end = now + (total * 1000);
    do {

//      /// one time print addition
//      if(printOnce)
//      {
//        printExecutorLocation(client);
//      }
//      printOnce = false;
//      ///

      metrics(client, now, state, "RUNNING");

      now = System.currentTimeMillis();
      cycle = (now - startTime)/pollMs;
      wakeupTime = startTime + (pollMs * (cycle + 1));
      sleepTime = wakeupTime - now;
      if (sleepTime > 0) {
        Thread.sleep(sleepTime);
      }
      now = System.currentTimeMillis();
    } while (now < end);
  }

  public boolean metrics(Nimbus.Client client, long now, MetricsState state, String message) throws Exception {
    ClusterSummary summary = client.getClusterInfo();
    long time = now - state.lastTime;
    state.lastTime = now;
    int numSupervisors = summary.get_supervisors_size();
    int totalSlots = 0;
    int totalUsedSlots = 0;

    //////////
    //String namaSupervisor = "";
    for (SupervisorSummary sup: summary.get_supervisors()) {
      totalSlots += sup.get_num_workers();
      totalUsedSlots += sup.get_num_used_workers();
      //namaSupervisor = namaSupervisor + sup.get_host() + ",";
    }
    //System.out.println(namaSupervisor);

    state.slotsUsed = totalUsedSlots;

    int numTopologies = summary.get_topologies_size();
    long totalTransferred = 0;
    int totalExecutors = 0;
    int executorsWithMetrics = 0;
    for (TopologySummary ts: summary.get_topologies()) {
      String id = ts.get_id();
      TopologyInfo info = client.getTopologyInfo(id);

      for (ExecutorSummary es: info.get_executors()) {
        ExecutorStats stats = es.get_stats();
        totalExecutors++;
        if (stats != null) {
          Map<String,Map<String,Long>> transferred = stats.get_emitted();/* .get_transferred();*/
          if ( transferred != null) {
            Map<String, Long> e2 = transferred.get(":all-time");
            if (e2 != null) {
              executorsWithMetrics++;
              //The SOL messages are always on the default stream, so just count those
              Long dflt = e2.get("default");
              if (dflt != null) {
                totalTransferred += dflt;
              }
            }
          }
        }
      }
    }

    state.transferred = totalTransferred;
    System.out.println(message+","+totalSlots+","+totalUsedSlots+","+totalExecutors+","+executorsWithMetrics+","+time+",NOLIMIT");
    return !(totalUsedSlots > 0 && totalExecutors > 0 && executorsWithMetrics >= totalExecutors);
  }

  public void realMain(String[] args) throws Exception {

    Map clusterConf = Utils.readStormConfig();
    clusterConf.putAll(Utils.readCommandLineOpts());
    Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();

    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
    } catch( CmdLineException e ) {
      // if there's a problem in the command line,
      // you'll get this exception. this will report
      // an error message.
      System.err.println(e.getMessage());
      _help = true;
    }
    if(_help) {
      parser.printUsage(System.err);
      System.err.println();
      return;
    }
    if (_numWorkers <= 0) {
      throw new IllegalArgumentException("Need at least one worker");
    }
    if (_name == null || _name.isEmpty()) {
      throw new IllegalArgumentException("name must be something");
    }

    try {
      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {

        int totalSpout = _spoutParallel * _localGroup;
        int totalLocalBolt = _boltLocalParallel * _localGroup;
        int totalLocalResultBolt = _localGroup;
        int totalGlobalBolt = _boltGlobalParallel;
        int totalGlobalResultBolt = 1;

        //configuration for rabbitMQ
        Declarator declarator = new SOERabbitDeclarator("", "accel", "accel");
        Scheme scheme = new SOERabbitMQScheme();
        ConnectionConfig connectionConfig = new ConnectionConfig("localhost", "guest", "guest");
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("accel")
                .prefetch(200)
                .build();

        TopologyBuilder builder = new TopologyBuilder();

        //Local1 TaskGroup
        builder.setSpout("RabbitMQSpoutLocal", new UnanchoredRabbitMQSpout(scheme, declarator), totalSpout)
                .addConfigurations(spoutConfig.asMap())
                .addConfiguration("group-name", "Local1")
                .setMaxSpoutPending(200);

        builder.setBolt("TupleSeparator", new SOETupleSeparatorBolt(), totalLocalBolt)
                .customGrouping("RabbitMQSpoutLocal", new ZoneShuffleGrouping())
                .addConfiguration("group-name", "Local1");

        builder.setBolt("CheckVibration_Local", new SOECheckVibrationBolt(), totalLocalBolt)
                .customGrouping("TupleSeparator", new ZoneShuffleGrouping())
                .addConfiguration("group-name", "Local1");

        if(_globalAlert) {
          builder.setBolt("AlertBolt_Local", new SOEFinalAlertBolt(), _localGroup)
                  .directGrouping("AlertForwarder_Global")
                  .addConfiguration("group-name", "Local1");
        }
        else {
          builder.setBolt("AlertBolt_Local", new SOEFinalAlertBolt(), _localGroup)
                  .customGrouping("CheckVibration_Local", new ZoneShuffleGrouping())
                  .addConfiguration("group-name", "Local1");
        }

        //Global1 TaskGroup
        builder.setBolt("CheckVibration_Global", new SOECheckVibrationBolt(), totalGlobalBolt)
                .shuffleGrouping("TupleSeparator")
                .addConfiguration("group-name", "Global1");

        builder.setBolt("AlertForwarder_Global", new SOEAlertForwarderBolt(), 1)
                .shuffleGrouping("CheckVibration_Global")
                .addConfiguration("group-name", "Global1");

//        builder.setBolt("messageBoltGlobal1_FG", new SOECheckVibrationBolt(), totalGlobalBolt)
//                .fieldsGrouping("messageBoltGlobal1_1A", new Fields("fieldValue"))
//                .addConfiguration("group-name", "Global1");
//
//        builder.setBolt("messageBoltGlobal1_GlobalResult", new SOEFinalAlertBolt(), totalGlobalResultBolt)
//                .shuffleGrouping("messageBoltGlobal1_FG")
//                .addConfiguration("group-name", "Global1");

        Config conf = new Config();
        conf.setDebug(_debug);
        conf.setNumWorkers(_numWorkers);
        conf.setNumAckers(_ackers);
        conf.setStatsSampleRate(_sampleRate);
        if (_maxSpoutPending > 0) {
          conf.setMaxSpoutPending(_maxSpoutPending);
        }

        StormSubmitter.submitTopology(_name+"_"+topoNum, conf, builder.createTopology());
      }
      metrics(client,  _pollFreqSec, _testRunTimeSec);
      //Thread.sleep(_testRunTimeSec * 1000);

    } finally {
      //Kill it right now!!!
      KillOptions killOpts = new KillOptions();
      killOpts.set_wait_secs(0);

      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {
        LOG.info("KILLING "+_name+"_"+topoNum);
        try {
          client.killTopologyWithOpts(_name+"_"+topoNum, killOpts);
        } catch (Exception e) {
          LOG.error("Error tying to kill "+_name+"_"+topoNum,e);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    new PiClusterTopology().realMain(args);
  }
}
