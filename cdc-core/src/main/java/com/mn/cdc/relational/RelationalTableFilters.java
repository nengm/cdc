package com.mn.cdc.relational;

import com.mn.cdc.config.Configuration;
import com.mn.cdc.relational.Selectors.TableIdToStringMapper;
import com.mn.cdc.schema.DataCollectionFilters;

import java.util.function.Predicate;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-30 15:57
 **/
public class RelationalTableFilters implements DataCollectionFilters {
    private final Tables.TableFilter tableFilter;
    private final Predicate<String> databaseFilter;

    public RelationalTableFilters(Configuration config, Tables.TableFilter systemTablesFilter, TableIdToStringMapper tableIdToStringMapper){
        Predicate<TableId > predicate = Selectors.tableSelector().includeDatabases(config.getString("table.whitelist"))
                .excludeDatabases(config.getString("table.blacklist")).build();
        this.tableFilter = (t)->predicate.and(systemTablesFilter::isIncluded).test(t);
        // Define the database filter using the include and exclude lists for database names ...
        this.databaseFilter = Selectors.databaseSelector()
                .includeDatabases(
                        config.getFallbackStringProperty(
                                RelationalDatabaseEngineConfig.DATABASE_INCLUDE_LIST,
                                RelationalDatabaseEngineConfig.DATABASE_WHITELIST))
                .excludeDatabases(
                        config.getFallbackStringProperty(
                                RelationalDatabaseEngineConfig.DATABASE_EXCLUDE_LIST,
                                RelationalDatabaseEngineConfig.DATABASE_BLACKLIST))
                .build();
    }

    @Override
    public Tables.TableFilter dataCollectionFilter() {
        return tableFilter;
    }

    public Predicate<String> databaseFilter() {
        return databaseFilter;
    }

    public void MyFilterTest(){
        Selectors.tableSelector().includeDatabases("miaon.mybaseinfotest")
                .excludeDatabases("miaon.mybaseinfotest2");
    }


}
