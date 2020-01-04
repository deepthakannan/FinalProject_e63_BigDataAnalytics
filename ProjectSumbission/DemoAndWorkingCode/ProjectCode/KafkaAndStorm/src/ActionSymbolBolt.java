/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

  public class ActionSymbolBolt extends BaseRichBolt {
    /**
	 * 
	 */
	private static CQLClient dbClient = null;
	public ActionSymbolBolt()
	{
		
	}
	  
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) 
    {
    		_collector = collector;
    	      synchronized (this) {
    	    	  dbClient = new CQLClient();
    	    	  dbClient.connect("127.0.0.1");
    	      }
    }

    @Override
    public void execute(Tuple tuple) {
    	String trade = tuple.getString(0);
    	String[] params = trade.split(":");
    	synchronized (this)
    	{
    	dbClient.loadData(params[1], params[0]);
      _collector.emit(tuple, new Values(params[0] + ":" + params[1]));
      _collector.ack(tuple);
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("ActionSymbol"));
    }

  
  }