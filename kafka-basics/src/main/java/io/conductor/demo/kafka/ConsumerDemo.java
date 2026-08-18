package io.conductor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private  static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Consumer Kafka Demo");
        String topic = "my-topic";
        String groupId = "my-group";

        Properties properties = new Properties();

        // key : value of properties
        properties.put("bootstrap.servers", "127.0.0.1:9092");

        //setting producer properties
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("group.id", groupId);
        properties.put("auto.offset.reset", "earliest");


        //creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribing to a topic
        consumer.subscribe(Arrays.asList(topic));

        //polling data from consumer

        while(true){

            log.info("Polling data ");

            ConsumerRecords<String,String> recordrs= consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:recordrs){
                log.info("key : "+record.key() +" value : "+record.value());
                log.info("partition : "+record.partition() +" offset : "+record.offset());
            }
        }


    }
}
