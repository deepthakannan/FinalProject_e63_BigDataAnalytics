import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
 
public class KafkaConsumerSpout {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
    KafkaStream<byte[], byte[]> kStream;
    ConsumerTest consumerTest;
    
    public KafkaConsumerSpout() {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = "page_visits";
        Initialize();
    }
 
    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }
 
    public void Initialize() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        kStream = consumerMap.get(topic).get(0);
 
        // now create an object to consume the messages
        //
        consumerTest = new ConsumerTest(kStream);
    }
    
    public String GetNextMessage()
    {
    	return consumerTest.GetNextMessage();
    }
 
    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("group.id", "cgroup123");
        props.put("zookeeper.session.timeout.ms", "4000000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
 
        return new ConsumerConfig(props);
    }
}