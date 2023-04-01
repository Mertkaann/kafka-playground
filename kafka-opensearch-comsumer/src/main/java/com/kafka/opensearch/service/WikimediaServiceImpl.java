package com.kafka.opensearch.service;

import com.kafka.opensearch.Model.Wikimedia;
import com.kafka.opensearch.repository.WikimediaRepository;
import lombok.Data;
import lombok.RequiredArgsConstructor;
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
