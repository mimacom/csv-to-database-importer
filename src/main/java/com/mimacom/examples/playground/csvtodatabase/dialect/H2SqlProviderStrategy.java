package com.mimacom.examples.playground.csvtodatabase.dialect;

import com.mimacom.examples.playground.csvtodatabase.TableConfigData;

import org.springframework.stereotype.Component;

@Component
class H2SqlProviderStrategy implements SqlProviderStrategy {

    private static final String DATABASE_PLATFORM = "H2";

    @Override
    public boolean supports(String datasourcePlatform) {
        return DATABASE_PLATFORM.equalsIgnoreCase(datasourcePlatform);
    }

    @Override
    public String sqlInsert(TableConfigData tableConfigData) {
        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                tableConfigData.getTableName(),
                tableConfigData.columns(),
                tableConfigData.params()
        );
    }

    @Override
    public String sqlTruncate(TableConfigData tableConfigData) {
        return String.format("truncate table %s", tableConfigData.getTableName());
    }
}
