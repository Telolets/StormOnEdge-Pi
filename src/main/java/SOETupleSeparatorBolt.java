/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SOETupleSeparatorBolt extends BaseRichBolt {
  /**
	 * 
	 */
	private static final long serialVersionUID = -7693495734028013915L;

  private static final Logger LOG = LoggerFactory.getLogger(SOETupleSeparatorBolt.class);
	
  private OutputCollector _collector;
  private String nodeName;
  private long timeStamp;
  private List<Integer> common;
  
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
    nodeName = context.getThisWorkerPort().toString() + "," + context.getThisTaskId();

    List<Integer> workerTasks = context.getThisWorkerTasks();
    List<Integer> alertBoltTasks = context.getComponentTasks("AlertBolt_Local");

    LOG.info("!!~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!");
    LOG.info("!![Prepare] worker tasks : " + workerTasks);
    LOG.info("!![Prepare] Alert bolt tasks : " + alertBoltTasks);

    common = new ArrayList<Integer>(workerTasks);
    common.retainAll(alertBoltTasks);

    LOG.info("!![Prepare] Common Task : " + common);
    LOG.info("!!~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!!");

    //context.addTaskHook(new HookBolt());
  }

  
  public void execute(Tuple tuple) {

    double x,y,z;

    timeStamp = System.currentTimeMillis();
    String message = tuple.getStringByField("message");

    //format:
    //0.1,0.2,0.3
    String[] arr = message.split(",");

    x = new Double(arr[0]);
    y = new Double(arr[1]);
    z = new Double(arr[2]);

    //LOG.info("!!Message from previous : " + message);

    _collector.emit(tuple, new Values(x,y,z, common, timeStamp));

	  _collector.ack(tuple);
  }

  @Override
  public void cleanup() {
  }


  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("Xvalue","Yvalue","Zvalue", "alertTaskIDs", "timeStamp"));
  }
}
