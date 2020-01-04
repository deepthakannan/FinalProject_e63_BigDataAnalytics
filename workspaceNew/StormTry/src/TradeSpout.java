import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.HashMap;
import java.util.Random;
import org.apache.log4j.Logger;


public class TradeSpout extends BaseRichSpout {
    public static Logger LOG = Logger.getLogger(TradeSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public TradeSpout() {
        this(true);
    }

    public TradeSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
        
    }
        
    public void nextTuple() {
        Utils.sleep(100);
        final String[] symbols = new String[] {"IBM", "FB", "AAPL", "MSFT", "GE", "GM"};
        final String[] actions = new String[] {"BUY", "SELL", "SHORT", "COVER"};
        final String[] exchanges = new String[] {"NASDAQ", "NYSE"};
        final Random rand = new Random();
        final String symbol = symbols[rand.nextInt(symbols.length)];
        final String action = actions[rand.nextInt(actions.length)];
        final String exchange = exchanges[rand.nextInt(exchanges.length)];
        _collector.emit(new Values(action + ":" +symbol + ":" + rand.nextInt() + ":" + exchange));
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }    
}