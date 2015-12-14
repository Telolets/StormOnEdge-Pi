import StormOnEdge.grouping.stream.ZoneShuffleGrouping;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
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

  @Option(name="--local", usage="run in local mode")
  private boolean _local = false;

  @Option(name="--messageSizeByte", aliases={"--messageSize"}, metaVar="SIZE",
          usage="size of the messages generated in bytes")
  private int _messageSize = 100;

  @Option(name="--numTopologies", aliases={"-n"}, metaVar="TOPOLOGIES",
          usage="number of topologies to run in parallel")
  private int _numTopologies = 1;

  @Option(name="--ClusterGroup", aliases={"--clusterGroup"}, metaVar="CLUSTERGROUP",
          usage="Number of cluster")
  private int _clusterGroup = 4;

  @Option(name="--localTaskGroup", aliases={"--localGroup"}, metaVar="LOCALGROUP",
          usage="number of initial local TaskGroup")
  private int _localGroup = 9;

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
  private int _ackers = 1;

  @Option(name="--maxSpoutPending", aliases={"--maxPending"}, metaVar="PENDING",
          usage="maximum number of pending messages per spout (only valid if acking is enabled)")
  private int _maxSpoutPending = -1;

  @Option(name="--name", aliases={"--topologyName"}, metaVar="NAME",
          usage="base name of the topology (numbers may be appended to the end)")
  private String _name = "test";

  @Option(name="--ackEnabled", aliases={"--ack"}, usage="enable acking")
  private boolean _ackEnabled = false;

  @Option(name="--pollFreqSec", aliases={"--pollFreq"}, metaVar="POLL",
          usage="How often should metrics be collected")
  private int _pollFreqSec = 30;

  @Option(name="--testTimeSec", aliases={"--testTime"}, metaVar="TIME",
          usage="How long should the benchmark run for.")
  private int _testRunTimeSec = 5 * 60;

  @Option(name="--sampleRateSec", aliases={"--sampleRate"}, metaVar="SAMPLE",
          usage="Sample rate for metrics (0-1).")
  private double _sampleRate = 0.3;


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
    if (!_ackEnabled) {
      _ackers = 0;
    }

    try {
      for (int topoNum = 0; topoNum < _numTopologies; topoNum++) {

        int totalSpout = _spoutParallel * _localGroup;
        int totalLocalBolt = _boltLocalParallel * _localGroup;
        int totalLocalResultBolt = _localGroup;
        int totalGlobalBolt = _boltGlobalParallel;
        int totalGlobalResultBolt = 1;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("messageSpoutLocal1", new SOESpout(_messageSize, _ackEnabled), totalSpout).addConfiguration("group-name", "Local1");
//        builder.setBolt("messageBoltLocal1_1", new SOEBolt(), totalLocalBolt).shuffleGrouping("messageSpoutLocal1").addConfiguration("group-name", "Local1");
//        builder.setBolt("messageBoltLocal1_LocalResult", new SOEFinalBolt(), totalLocalResultBolt).shuffleGrouping("messageBoltLocal1_1").addConfiguration("group-name", "Local1");
        builder.setBolt("messageBoltLocal1_1", new SOEBolt(), totalLocalBolt).customGrouping("messageSpoutLocal1", new ZoneShuffleGrouping()).addConfiguration("group-name", "Local1");
        builder.setBolt("messageBoltLocal1_LocalResult", new SOEFinalBolt(), totalLocalResultBolt).customGrouping("messageBoltLocal1_1", new ZoneShuffleGrouping()).addConfiguration("group-name", "Local1");

//        builder.setSpout("messageSpoutLocal2", new SOESpout(_messageSize, _ackEnabled), totalSpout).addConfiguration("group-name", "Local2");
//        builder.setBolt("messageBoltLocal2_1", new SOEBolt(), totalLocalBolt).shuffleGrouping("messageSpoutLocal2").addConfiguration("group-name", "Local2");
//        builder.setBolt("messageBoltLocal2_LocalResult", new SOEFinalBolt(), totalLocalResultBolt).shuffleGrouping("messageBoltLocal2_1").addConfiguration("group-name", "Local2");
//        builder.setBolt("messageBoltLocal2_1", new SOEBolt(), totalLocalBolt).customGrouping("messageSpoutLocal2", new ZoneShuffleGrouping()).addConfiguration("group-name", "Local2");
//        builder.setBolt("messageBoltLocal2_LocalResult", new SOEFinalBolt(), totalLocalResultBolt).customGrouping("messageBoltLocal2_1", new ZoneShuffleGrouping()).addConfiguration("group-name", "Local2");

        builder.setBolt("messageBoltGlobal1_1A", new SOEBolt(), totalGlobalBolt).shuffleGrouping("messageBoltLocal1_1").addConfiguration("group-name", "Global1");
//        builder.setBolt("messageBoltGlobal1_1B", new SOEBolt(), totalGlobalBolt).shuffleGrouping("messageBoltLocal2_1").addConfiguration("group-name", "Global1");
        builder.setBolt("messageBoltGlobal1_FG", new SOEBolt(), 2)
                .fieldsGrouping("messageBoltGlobal1_1A", new Fields("fieldValue"))
//          .fieldsGrouping("messageBoltGlobal1_1B", new Fields("fieldValue"))
                .addConfiguration("group-name", "Global1");
        builder.setBolt("messageBoltGlobal1_GlobalResult", new SOEFinalBolt(), totalGlobalResultBolt)
                .shuffleGrouping("messageBoltGlobal1_FG")
                .addConfiguration("group-name", "Global1");

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
      //metrics(client, _messageSize, _pollFreqSec, _testRunTimeSec);
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
