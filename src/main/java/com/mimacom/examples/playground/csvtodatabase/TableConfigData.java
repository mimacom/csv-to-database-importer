package com.mimacom.examples.playground.csvtodatabase;

import java.util.List;
import java.util.stream.Collectors;

public class TableConfigData {

    private String fileName;

    private String tableName;

    private List<String> columns;

    String getFileName() {
        return fileName;
    }

    public TableConfigData setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    String getTableName() {
        return tableName;
    }

    public TableConfigData setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    List<String> getColumns() {
        return columns;
    }

    public TableConfigData setColumns(List<String> columns) {
        this.columns = columns;
        return this;
    }

    String columns() {
        return String.join(",", this.columns);
    }

    String params() {
        return columns.stream()
                .map(s -> ":" + s)
                .collect(Collectors.joining(","));
    }
}
