package com.mimacom.examples.playground.csvtodatabase.dialect;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
class SqlProviderConfiguration {

    private final List<SqlProviderStrategy> sqlProviderStrategies;

    private final String datasourcePlatform;

    SqlProviderConfiguration(List<SqlProviderStrategy> sqlProviderStrategies, Environment environment) {
        this.sqlProviderStrategies = sqlProviderStrategies;
        this.datasourcePlatform = environment.getProperty("spring.datasource.platform");
    }

    @Bean
    SqlProviderStrategy sqlProviderStrategy() {
        return this.sqlProviderStrategies.stream()
                .filter(sqlProviderStrategy -> sqlProviderStrategy.supports(this.datasourcePlatform))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Could not find a suitable strategy for database platform: " + this.datasourcePlatform));
    }
}
