package com.mimacom.examples.playground.csvtodatabase;

import java.util.HashMap;
import java.util.Map;

public class CsvEntry {

    private Map<String, Object> content = new HashMap<>();

    void add(String columName, Object value) {
        this.content.put(columName, value);
    }

    Map<String, Object> getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "CsvEntry{" +
                "content=" + content +
                '}';
    }

}
