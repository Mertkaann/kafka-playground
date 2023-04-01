package com.kafka.opensearch;

import com.kafka.opensearch.service.WikimediaService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
@Slf4j
@Data
public class MongoDBRunner implements CommandLineRunner {

    @Autowired
    private WikimediaService wikimediaService;

    public static KafkaConsumer<String,String> createKafkaConsumer(){
        Properties properties = new Properties();

        //Connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "opensearch-consumer");
        properties.setProperty("auto.offset.reset", "latest");

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String data){
        //gson
        return data;
    }

    @Override
    public void run(String... args) {

        //create kafka consumer
        KafkaConsumer<String,String> kafkaConsumer = createKafkaConsumer();

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling kafkaConsumer.wakeup()...");
                kafkaConsumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            kafkaConsumer.subscribe(Collections.singleton("wikimedia-data-topic3"));

            while (true){
                ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

                int recordCount = records.count();

                log.info("receiving " + recordCount + " records.");

                for(ConsumerRecord<String,String> record : records){

                    wikimediaService.saveWikimedia(record.value());
                }

            }

        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            kafkaConsumer.close(); // close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shut down");
        }
    }
}
