package com.kafka.mongodb;

import com.kafka.mongodb.repository.WikimediaRepository;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication
@EnableMongoRepositories(basePackageClasses = WikimediaRepository.class)
public class MongoDBConsumer {
    public static void main(String[] args) {
        SpringApplication.run(MongoDBConsumer.class, args);

    }
}