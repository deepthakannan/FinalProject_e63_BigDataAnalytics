import java.util.*;

 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class TestProducer {
    public static void main(String[] args) throws InterruptedException {
    	long events =0;
    	if(args == null || args.length == 0)
    		events = 100;
    	else
        events = Long.parseLong(args[0]);
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = GetNextTrade();
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
               Thread.sleep(100);
               producer.send(data);
        }
        producer.close();
    }
    
    private static String GetNextTrade()
    {
        final String[] symbols = new String[] {"IBM", "FB", "AAPL", "MSFT", "GE", "GM"};
        final String[] actions = new String[] {"BUY", "SELL", "SHORT", "COVER"};
        final String[] exchanges = new String[] {"NASDAQ", "NYSE"};
        final Random rand = new Random();
        final String symbol = symbols[rand.nextInt(symbols.length)];
        final String action = actions[rand.nextInt(actions.length)];
        final String exchange = exchanges[rand.nextInt(exchanges.length)];
        return action + ":" + symbol + ":" + rand.nextInt(1000) * 10 + ":" + exchange;
    }
}