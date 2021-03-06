package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
  public static void main(String[] args) {
    // Create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create Producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // Create a ProducerRecord
    ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "from kafka client");

    // Send Data - async
    producer.send(record);

    // Flush data
    producer.flush();
    // Flush and close producer
    producer.close();
  }
}
