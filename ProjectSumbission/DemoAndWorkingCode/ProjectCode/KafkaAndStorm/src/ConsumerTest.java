import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class ConsumerTest{
    private KafkaStream<byte[], byte[]> m_stream;
 
    public ConsumerTest(KafkaStream<byte[], byte[]> a_stream) {
        m_stream = a_stream;
    }
 
    public String GetNextMessage() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext())
        {
        	String message= new String(it.next().message());
            return message;
        }
        return "";
    }
}