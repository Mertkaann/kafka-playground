package com.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
@AllArgsConstructor
public class WikimediaChangeHandler implements EventHandler {

    private String topic;
    private KafkaProducer<String,String> producer;

    @Override
    public void onOpen(){
        log.info("Connection established with Wikimedia");

    }

    @Override
    public void onClosed() {
        producer.close();

    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        log.info("Message received: {}",messageEvent.getData());
        String data = messageEvent.getData();
        data = data.replace("$schema","s$chema");
        ProducerRecord<String,String> record = new ProducerRecord<>(topic,data );
        producer.send(record);
    }

    @Override
    public void onComment(String s) {

    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Unexpected error occurred" + throwable.getMessage());
    }
}
