package com.mimacom.examples.playground.csvtodatabase.dialect;

import com.mimacom.examples.playground.csvtodatabase.TableConfigData;

public interface SqlProviderStrategy {

    boolean supports(String datasourcePlatform);

    String sqlInsert(TableConfigData tableConfigData);

    String sqlTruncate(TableConfigData tableConfigData);
}
