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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

public class SOEFinalAlertBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7418734359875255034L;
  private static final Logger LOG = LoggerFactory.getLogger(SOEFinalAlertBolt.class);
  private OutputCollector _collector;

  private final String PythonLOCATION = "/home/pi/SOE/stormData";
  private final String PythonFILE1 = "alert.py";
  private final String PythonFILE2 = "reset.py";
  private String nodeID;

  public SOEFinalAlertBolt() {
    //Empty
  }

  
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
    nodeID = context.getThisWorkerPort().toString();

//    context.addTaskHook(new HookFinalBolt());

//    File file = new File("");
//    LOG.info("!!~~~~~~~~~~~~~~!!");
//    LOG.info(file.getAbsolutePath());
//    LOG.info("!!~~~~~~~~~~~~~~!!");
  }

  
  public void execute(Tuple tuple) {

    String message = tuple.getStringByField("message");
    long timeStamp = tuple.getLongByField("timeStamp");


    LOG.info("!!Something Happened in " + nodeID);
    LOG.info("!!At : " + new DateTime(timeStamp).toString());
    LOG.info("!!Current Time : " + DateTime.now());

    try {
      ProcessBuilder builder = new ProcessBuilder("python", PythonFILE1);
      builder.directory(new File(PythonLOCATION));
      Process process = builder.start();
//      process.waitFor();

//      builder = new ProcessBuilder("python", PythonFILE2);
//      builder.directory(new File(PythonLOCATION));
//      process = builder.start();
//      process.waitFor();

    }catch (Exception e) {
      LOG.info("[Final Alert]\n" + e.getMessage());
    }

	  _collector.ack(tuple);
  }

  @Override
  public void cleanup() {
  }


  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //declarer.declare(new Fields("message", "fieldValue", "timeStamp"));
  }
}
