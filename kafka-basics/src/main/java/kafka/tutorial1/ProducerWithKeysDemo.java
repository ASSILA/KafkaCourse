package kafka.tutorial1;

import kafka.solution1.ProducerDemoWithCallback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeysDemo {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    // Create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create Producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    for (int i = 0; i < 10; i++) {
      String topic = "first_topic";
      String value = "hello world" + i;
      String key = "id_" + i;

      // Create a ProducerRecord
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

      logger.info("Key: " + key);

      // Send Data - async
      producer.send(record, (recordMetadata, e) -> {
        // Executes every time record is successfully sent or an exception is thrown
        if (e == null) {
          // Record was sent
          logger.info("Received new metadata. \n" +
            "Topic: " + recordMetadata.topic() + "\n" +
            "Partition: " + recordMetadata.partition() + "\n" +
            "Offset: " + recordMetadata.offset() + "\n" +
            "Timestamp: " + recordMetadata.timestamp());
        } else {
          // Exception thrown
          logger.error("Error while producing", e);
        }
      }).get(); // block send to make it sync
    }
    // Flush data
    producer.flush();
    // Flush and close producer
    producer.close();
  }
}
