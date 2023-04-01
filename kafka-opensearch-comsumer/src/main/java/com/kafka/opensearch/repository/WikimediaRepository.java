package com.kafka.opensearch.repository;


import com.kafka.opensearch.Model.Wikimedia;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface WikimediaRepository extends MongoRepository<Wikimedia, UUID> {
}
