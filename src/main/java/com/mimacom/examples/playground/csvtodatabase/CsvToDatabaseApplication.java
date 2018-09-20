package com.mimacom.examples.playground.csvtodatabase;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class CsvToDatabaseApplication {

    public static void main(String[] args) {
        SpringApplication.run(CsvToDatabaseApplication.class, args);
    }
}
