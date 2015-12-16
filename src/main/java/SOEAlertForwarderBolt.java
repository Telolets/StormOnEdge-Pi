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

import java.util.List;
import java.util.Map;
import java.util.Random;

public class SOEAlertForwarderBolt extends BaseRichBolt {
  /**
	 * 
	 */
	private static final long serialVersionUID = -7693495013915L;
  private static final Logger LOG = LoggerFactory.getLogger(SOEAlertForwarderBolt.class);
  private List<Integer> alertBoltTasks;
	
  private OutputCollector _collector;
  
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
    //context.addTaskHook(new HookBolt());
    alertBoltTasks = context.getComponentTasks("AlertBolt_Local");
    LOG.info("[AlertForwarder] Ready: " + alertBoltTasks);
  }

  
  public void execute(Tuple tuple) {
    @SuppressWarnings("unchecked")
    List<Integer> destinationTasks = (List<Integer>) tuple.getValueByField("alertTaskIDs");
    LOG.info("[AlertForwarder] execute: " + destinationTasks);

    for(Integer taskID : destinationTasks) {

      LOG.info("[AlertForwarder] Send direct grouping to " + taskID);

      _collector.emitDirect(taskID, tuple,
              new Values(tuple.getStringByField("message"),
                      tuple.getValueByField("alertTaskIDs"),
                      tuple.getValueByField("timeStamp")));
    }

    _collector.ack(tuple);
  }

  @Override
  public void cleanup() {
  }


  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message", "alertTaskIDs", "timeStamp"));
  }
}
