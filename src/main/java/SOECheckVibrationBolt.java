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

import java.util.Map;
import java.util.Random;

public class SOECheckVibrationBolt extends BaseRichBolt {
  /**
	 * 
	 */
	private static final long serialVersionUID = -7693495734028013915L;
  private final String message = "Vibration Detected";
  private double maxTolerance = 1.04;
	
  private OutputCollector _collector;
  private Random rand;
  
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
    rand = new Random();

    //context.addTaskHook(new HookBolt());
  }

  
  public void execute(Tuple tuple) {

    double x = tuple.getDoubleByField("Xvalue");
    double y = tuple.getDoubleByField("Yvalue");
    double z = tuple.getDoubleByField("Zvalue");

    if (x > maxTolerance || y > maxTolerance || z > maxTolerance ) {
      _collector.emit(tuple, new Values(message,
              tuple.getValueByField("fieldValue"),
              tuple.getValueByField("timeStamp")));
    }

    _collector.ack(tuple);
  }

  @Override
  public void cleanup() {
  }


  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message", "fieldValue", "timeStamp"));
  }
}
