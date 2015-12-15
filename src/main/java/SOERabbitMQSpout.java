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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;

import java.util.Map;
import java.util.Random;

public class SOERabbitMQSpout extends BaseRichSpout {
	
/**
	 * 
	 */
	private static final long serialVersionUID = 4137062886055644678L;

  private long _messageCount;
  private SpoutOutputCollector _collector;
  private String [] _messages = null;
  private boolean _ackEnabled;
  private Random _rand = null;
  private String nodeName;
  private long timeStamp;
  
  public SOERabbitMQSpout(boolean ackEnabled) {
    _messageCount = 0;
    _ackEnabled = ackEnabled;
  }

  public boolean isDistributed() {
    return true;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _rand = new Random();
    _collector = collector;

    nodeName = context.getThisWorkerPort().toString() + "," + context.getThisTaskId();
    
    context.addTaskHook(new HookSpout());
  }

  @Override
  public void close() {
    //Empty
  }

  public void nextTuple() {
    final String message = _messages[_rand.nextInt(_messages.length)];
    timeStamp = System.currentTimeMillis();
    //String fieldValue = String.valueOf(_rand.nextInt(10));
    
    if (_messageCount < 40000)
    {
	    if(_ackEnabled) {
	      _collector.emit(new Values(message, nodeName, timeStamp), _messageCount);
	    } else {
	      _collector.emit(new Values(message, nodeName, timeStamp));
	    }
	    _messageCount++;
    }
    try {
    	Thread.sleep(2, 0);
    } catch(Exception e) { }
  }


  @Override
  public void ack(Object msgId) {
    //Empty
  }

  @Override
  public void fail(Object msgId) {
    //Empty
  }

public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// TODO Auto-generated method stub
	declarer.declare(new Fields("message", "fieldValue", "timeStamp"));
}
  
  
}
