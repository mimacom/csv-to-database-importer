package com.mimacom.examples.playground.csvtodatabase;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("importer")
@Validated
public class ImporterProperties {

    @NotNull
    private Resource csvFilesBaseResource;

    @NotNull
    private Resource tableConfigResource = new ClassPathResource("tables-to-import.json");

    private int numberOfParallelTableImports = 1;

    @Min(10)
    private int chunkSize = 100;

    public Resource getCsvFilesBaseResource() {
        return csvFilesBaseResource;
    }

    public void setCsvFilesBaseResource(Resource csvFilesBaseResource) {
        this.csvFilesBaseResource = csvFilesBaseResource;
    }

    public Resource getTableConfigResource() {
        return tableConfigResource;
    }

    public void setTableConfigResource(Resource tableConfigResource) {
        this.tableConfigResource = tableConfigResource;
    }

    public int getNumberOfParallelTableImports() {
        return numberOfParallelTableImports;
    }

    public ImporterProperties setNumberOfParallelTableImports(int numberOfParallelTableImports) {
        this.numberOfParallelTableImports = numberOfParallelTableImports;
        return this;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public ImporterProperties setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }
}
