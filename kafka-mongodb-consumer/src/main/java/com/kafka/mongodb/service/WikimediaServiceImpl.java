package com.kafka.mongodb.service;

import com.kafka.mongodb.Model.Wikimedia;
import com.kafka.mongodb.repository.WikimediaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class WikimediaServiceImpl implements WikimediaService {

    @Autowired
    private WikimediaRepository wikimediaRepository;

    @Override
    public void saveWikimedia(String value){

        Wikimedia wikimedia = new Wikimedia();
        wikimedia.setId(UUID.randomUUID());
        wikimedia.setValue(value);

        wikimediaRepository.save(wikimedia);
    }

}
