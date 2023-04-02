package com.kafka.mongodb.Model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.UUID;

@Document(value = "wikimedia")
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class Wikimedia {

    @Id
    private UUID id;
    private String value;

}
