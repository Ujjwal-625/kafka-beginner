package io.conducktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> producer;
    String topic;

    Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class);

    public WikimediaChangeHandler(KafkaProducer<String , String> producer , String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen()  {
        //do nothing
    }

    @Override
    public void onClosed()  {
        //close the producer
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {

        logger.info("WikimediaChangeHandler received an event: " + event);
        logger.info("WikimediaChangeHandler received a message: " + messageEvent.getData().toString());
        producer.send( new ProducerRecord<>(topic,messageEvent.getData().toString()));
    }

    @Override
    public void onComment(String comment)  {
    //nothing
    }

    @Override
    public void onError(Throwable t) {
        logger.error("WikimediaChangeHandler received an error", t);
    }
}
