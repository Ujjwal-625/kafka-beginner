package io.conductor.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private  static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Kafka Demo");

        Properties properties = new Properties();

        // key : value of properties
        properties.put("bootstrap.servers", "127.0.0.1:9092");

        //setting producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //creating producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // creating producer code
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-topic", "First Message");

        //send data
        producer.send(record);

        // flush and close the producer
        producer.flush();
        producer.close();
    }
}
