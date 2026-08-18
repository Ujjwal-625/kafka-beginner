package io.conductor.demo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {

    private  static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKey.class.getSimpleName());

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

        //sending multiple messages

        for(int j=0;j<2;j++){
            for(int i=0;i<10;i++){
                String topic = "my-topic";
                String key = "key"+i;
                String value = "value"+i;
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

                //send data
                producer.send(record, new Callback() {
                    //this will run whenever record is succesfully sent or exception is thrown
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception!=null){
                            log.error("Error sending record to topic",exception);
                        }
                        else {
                            log.info("Key : "+key + " Partition : "+metadata.partition()+" Offset : "+metadata.offset());

                        }
                    }
                });
            }
        }


        // flush and close the producer
        producer.flush();
        producer.close();
    }
}
