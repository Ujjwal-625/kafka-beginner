package io.conducktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducers {
    public static void main(String[] args) throws InterruptedException {
        String BootstrapServers = "localhost:9092";

        Properties properties = new Properties();
        properties.put("bootstrap.servers", BootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentChanges";

       EventHandler event = new WikimediaChangeHandler(producer, topic);

        String url ="https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(event , URI.create(url)).headers(new okhttp3.Headers.Builder()
                .add("User-Agent", "WikimediaKafkaProducer/1.0")
                .build());
        EventSource eventSource = builder.build();

        eventSource.start();

        //now stop the main thread for 10 min
        TimeUnit.MINUTES.sleep(10);
    }
}
