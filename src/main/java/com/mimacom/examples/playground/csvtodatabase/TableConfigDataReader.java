package com.mimacom.examples.playground.csvtodatabase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.core.io.Resource;

public class TableConfigDataReader {

    private final ObjectMapper objectMapper;

    private final Resource tableConfigResource;

    public TableConfigDataReader(Resource tableConfigResource) {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.findAndRegisterModules();
        this.tableConfigResource = tableConfigResource;
    }

    List<TableConfigData> read() throws IOException {
        try (InputStream inputStream = this.tableConfigResource.getInputStream()) {
            return this.objectMapper.readValue(inputStream, new TypeReference<List<TableConfigData>>() {});
        }
    }
}
