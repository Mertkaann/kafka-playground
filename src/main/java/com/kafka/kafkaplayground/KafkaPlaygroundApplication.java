package com.kafka.kafkaplayground;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
@Slf4j
public class KafkaPlaygroundApplication {

    private static String getMetaDataString(RecordMetadata metadata){
        return  "Topic:" + metadata.topic()+"\n"+
                "Partition:" +  metadata.partition()+"\n"+
                "Offset:" +  metadata.offset()+"\n"+
                "Timestamp:" +  metadata.timestamp()+"\n";
    }

    public static void main(String[] args) {
        log.info("Starting Kafka Playground");

        Properties properties = new Properties();

        //Connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");


        /***Connect to conduktor***/
        //properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        //properties.setProperty("security.protocol", "SASL_SSL");
        //properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"your-username\" password=\"your-password\";");
        //properties.setProperty("sasl.mechanism", "PLAIN");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //Create producer Record with Callback
        for(int j=0;j<2;j++) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("demo_kafka", "key_"+j, "Hello World " + i);
                producer.send(record, (recordMetadata, e) -> {
                    if (e != null) {
                        log.error("Failed to send record", e);
                        log.error(recordMetadata.toString());
                    } else {
                        log.info("Successfully sent record");
                        log.info(getMetaDataString(recordMetadata));
                    }
                });
            }
        }

        producer.flush();

        producer.close();


        SpringApplication.run(KafkaPlaygroundApplication.class, args);
    }

}
