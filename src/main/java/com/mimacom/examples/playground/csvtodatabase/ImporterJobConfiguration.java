package com.mimacom.examples.playground.csvtodatabase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.mimacom.examples.playground.csvtodatabase.dialect.SqlProviderStrategy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Configuration
@EnableConfigurationProperties(ImporterProperties.class)
public class ImporterJobConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImporterJobConfiguration.class);

    private final ImporterProperties importerProperties;

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final JdbcTemplate jdbcTemplate;

    private final SqlProviderStrategy sqlProviderStrategy;

    public ImporterJobConfiguration(ImporterProperties importerProperties, JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, JdbcTemplate jdbcTemplate, SqlProviderStrategy sqlProviderStrategy) {
        this.importerProperties = importerProperties;
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jdbcTemplate = jdbcTemplate;
        this.sqlProviderStrategy = sqlProviderStrategy;
    }

    @Bean
    public TableConfigDataReader tableConfigDataReader() {
        return new TableConfigDataReader(this.importerProperties.getTableConfigResource());
    }

    @Bean
    public Job csvImportJob() throws IOException {
        return this.jobBuilderFactory.get("job")
                .start(importingStep())
                .build();
    }

    @Bean
    Step importingStep() throws IOException {
        List<TableConfigData> tableConfigData = this.tableConfigDataReader().read();
        return this.stepBuilderFactory.get("importing-step")
                .flow(new FlowBuilder<Flow>("parallel-table-importer-flows")
                        .split(parallelTableImportTaskExecutor())
                        .add(tableConfigData.stream()
                                .map(this::tableImportingFlow)
                                .toArray(Flow[]::new))
                        .build())
                .build();
    }

    private Flow tableImportingFlow(TableConfigData tableConfigData) {
        return new FlowBuilder<Flow>("importing-flow-" + tableConfigData.getTableName())
                .start(truncateTableStep(tableConfigData))
                .next(importTableStep(tableConfigData))
                .build();
    }

    private TaskletStep truncateTableStep(TableConfigData tableConfigData) {
        return this.stepBuilderFactory.get("truncate-" + tableConfigData.getTableName())
                .tasklet((contribution, chunkContext) -> {
                    LOGGER.info("Truncate Table: {} ", tableConfigData.getTableName());
                    this.jdbcTemplate.execute(sqlProviderStrategy.sqlTruncate(tableConfigData));
                    return null;
                })
                .build();
    }

    private Step importTableStep(TableConfigData tableConfigData) {
        return this.stepBuilderFactory.get("import-" + tableConfigData.getTableName())
                .<CsvEntry, Map<String, Object>>chunk(this.importerProperties.getChunkSize())
                .reader(this.csvReader(tableConfigData))
                .processor((ItemProcessor<CsvEntry, Map<String, Object>>) CsvEntry::getContent)
                .writer(this.jdbcTemplateWriter(tableConfigData))
                .build();
    }

    private ItemWriter<Map<String, Object>> jdbcTemplateWriter(TableConfigData tableConfigData) {
        String sql = prepareSql(tableConfigData);
        JdbcBatchItemWriter<Map<String, Object>> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
        jdbcBatchItemWriter.setJdbcTemplate(new NamedParameterJdbcTemplate(this.jdbcTemplate));
        jdbcBatchItemWriter.setDataSource(this.jdbcTemplate.getDataSource());
        jdbcBatchItemWriter.setSql(sql);
        jdbcBatchItemWriter.afterPropertiesSet();
        return jdbcBatchItemWriter;
    }

    private String prepareSql(TableConfigData tableConfigData) {
        return sqlProviderStrategy.sqlInsert(tableConfigData);
    }

    private ItemReader<CsvEntry> csvReader(TableConfigData tableConfigData) {
        FlatFileItemReader<CsvEntry> flatFileItemReader = new FlatFileItemReader<>();
        FileSystemResource resource = new FileSystemResource(buildPath(tableConfigData));
        flatFileItemReader.setResource(resource);
        flatFileItemReader.setLinesToSkip(1);
        DefaultLineMapper<CsvEntry> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(DelimitedLineTokenizer.DELIMITER_COMMA);
        delimitedLineTokenizer.setQuoteCharacter(DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER);
        delimitedLineTokenizer.setNames(tableConfigData.getColumns().toArray(new String[0]));
        lineMapper.setLineTokenizer(delimitedLineTokenizer);
        lineMapper.setFieldSetMapper(fieldSet -> {
            CsvEntry csvEntry = new CsvEntry();
            String[] names = fieldSet.getNames();
            String[] values = fieldSet.getValues();
            for (int i = 0; i < names.length; i++) {
                String name = names[i];
                String value = values[i];
                csvEntry.add(name, nullAwareStingValue(value));
            }
            return csvEntry;
        });
        flatFileItemReader.setLineMapper(lineMapper);
        try {
            flatFileItemReader.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException("", e);
        }
        return flatFileItemReader;
    }

    private String nullAwareStingValue(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        if (value.trim().toLowerCase().equals("null")) {
            return null;
        }
        return value;
    }

    private String buildPath(TableConfigData tableConfigData) {
        try {
            return importerProperties.getCsvFilesBaseResource().getFile().getPath() + "/" + tableConfigData.getFileName();
        } catch (IOException e) {
            throw new IllegalStateException("", e);
        }
    }

    private SimpleAsyncTaskExecutor parallelTableImportTaskExecutor() {
        SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
        taskExecutor.setConcurrencyLimit(this.importerProperties.getNumberOfParallelTableImports());
        taskExecutor.setThreadGroupName("table-import-grp");
        taskExecutor.setThreadNamePrefix("table-import-");
        return taskExecutor;
    }
}
