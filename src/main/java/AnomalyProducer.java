import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AnomalyProducer {
    Producer<String, String> producer;
    String topic;

    public AnomalyProducer(String topic){
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.3.4:9092");
        props.put("acks", "all");
        props.put("retires", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<String, String>(props);
        this.topic = topic;
    }

    public void send(String key, String msg){
        this.producer.send(new ProducerRecord<>(this.topic, key, msg));
    }

}
