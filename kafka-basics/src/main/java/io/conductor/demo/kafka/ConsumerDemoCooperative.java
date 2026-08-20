package io.conductor.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private  static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

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
        properties.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

//        properties.put("group.instance.id","groupId");// to make static consumer


        //creating consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutting down Kafka Consumer");
                consumer.wakeup();


                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

       try{
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
       }catch(WakeupException e){
           log.info("consumer starting to shutdown");
       }
       catch (Exception e){
           log.info("unexpected exception");
       }
       finally {
           consumer.close(); //close the consumer , this will also commit offsets
           log.info("consumer shutdown complete");
       }


    }
}
