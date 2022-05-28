package com.mn.cdc.oracle;

import com.mn.cdc.config.Configuration;
import com.mn.cdc.config.Field;
import com.mn.cdc.iengine.IEngine;
import com.mn.cdc.jdbc.JdbcConfiguration;
import com.mn.cdc.jdbc.JdbcConnection;
import com.mn.cdc.oracle.conf.LogMinerConf;
import com.mn.cdc.structure.BaseRecord;
import com.mn.cdc.structure.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-12-03 10:37
 **/
public class OracleLogminerEngine implements IEngine {
    Logger logger = LoggerFactory.getLogger(OracleLogminerEngine.class);

    private LogMinerConf logMinerConf;
    LogminerSession logminerSession;
    @Override
    public void start(Configuration config, Map<String, ?> offsets) {
        logMinerConf = new LogMinerConf();
        Configuration jdbcConfig = config.subset("database.", true);
        Properties props = jdbcConfig.asProperties();
        Field[] varsWithDefaults = JdbcConnection.combineVariables(null,
                JdbcConfiguration.HOSTNAME,
                JdbcConfiguration.PORT,
                JdbcConfiguration.USER,
                JdbcConfiguration.PASSWORD,
                JdbcConfiguration.DATABASE);
        String baseUrl = "jdbc:oracle:thin:@${" + JdbcConfiguration.HOSTNAME + "}:${" + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";
        String url = JdbcConnection.findAndReplace(baseUrl, props, varsWithDefaults);
        String username = props.getProperty(JdbcConfiguration.USER.name());
        String password = props.getProperty(JdbcConfiguration.PASSWORD.name());
        logMinerConf.setJdbcUrl(url);
        logMinerConf.setUsername(username);
        logMinerConf.setPassword(password);

        logminerSession = new LogminerSession(logMinerConf);

        //主动连接
        logminerSession.connect();
    }

    @Override
    public List<BaseRecord> poll() throws InterruptedException {

        return logminerSession.poll();
    }
}
