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


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class TradeInfoTopology {
	private static CQLClient dbClient = null;
  public static class BuySellCount extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      synchronized (this) {
    	  dbClient = new CQLClient();
    	  dbClient.connect("127.0.0.1");
	}
      
    }
    
    Map<String, Long> counts = new HashMap<String, Long>(); 

    @Override
    public void execute(Tuple tuple) {
    	synchronized(this)
    	{
    	String actionSymbol = tuple.getString(0);
    	String[] actionAndSymbol = actionSymbol.split(":");
    	Long count = counts.get(actionSymbol);
    	if(count == null) 
    		count = (long) 0;
    	counts.put(actionSymbol, ++count);
    	synchronized (dbClient) {
    		dbClient.updateFrequency(actionAndSymbol[1], actionAndSymbol[0], count);
		}
    	
    	_collector.emit(tuple, new Values(actionAndSymbol[0], actionAndSymbol[1], count));
      _collector.ack(tuple);
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("Action", "Security" , "Count"));
    }

    
  }

  public static void main(String[] args) throws Exception {
	  
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("trade", new TradeSpout(), 10);
    builder.setBolt("actionsymbol", new ActionSymbolBolt(), 3).shuffleGrouping("trade");
    builder.setBolt("BuySellCount", new BuySellCount(), 2).fieldsGrouping("actionsymbol", new Fields("ActionSymbol"));

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(60000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}