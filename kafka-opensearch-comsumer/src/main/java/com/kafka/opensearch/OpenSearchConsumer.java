package com.kafka.opensearch;

import com.kafka.opensearch.repository.WikimediaRepository;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;



@SpringBootApplication
@EnableMongoRepositories(basePackageClasses = WikimediaRepository.class)
public class OpenSearchConsumer {

    public static void main(String[] args) {

        SpringApplication.run(OpenSearchConsumer.class, args);

    }


}
