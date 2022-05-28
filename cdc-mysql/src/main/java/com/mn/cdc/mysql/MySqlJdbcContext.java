package com.mn.cdc.mysql;

import com.mn.cdc.config.CommonEngineConfig;
import com.mn.cdc.config.Configuration;
import com.mn.cdc.config.Field;
import com.mn.cdc.exception.CdcException;
import com.mn.cdc.exception.ConnectException;
import com.mn.cdc.jdbc.JdbcConnection;
import com.mn.cdc.mysql.config.MySqlEngineConfig;
import com.mn.cdc.mysql.config.MySqlEngineConfig.*;
import com.mn.cdc.relational.history.DatabaseHistory;
import com.mn.cdc.util.EnumUtil.*;
import com.mn.cdc.util.GtidSet;
import com.mn.cdc.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @program:cdc-master
 * @description jdbc连接上下文
 * @author:miaoneng
 * @create:2021-09-07 09:28
 **/
public class MySqlJdbcContext implements AutoCloseable {

    private final static Logger logger = LoggerFactory.getLogger(MySqlJdbcContext.class);

    protected final JdbcConnection jdbcConnection;

    protected final Configuration config;
    //mysql连接
    protected static final String MYSQL_CONNECTION_URL = "jdbc:mysql://${hostname}:${port}/?useInformationSchema=true&nullCatalogMeansCurrent=false&useSSL=${useSSL}&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=Asia/Shanghai";

    private static final String SQL_SHOW_SYSTEM_VARIABLES = "SHOW VARIABLES";

    //不用旧时区，统一设置为false
    protected static final String JDBC_PROPERTY_LEGACY_DATETIME = "useLegacyDatetimeCode";

    private static final String SQL_SHOW_SYSTEM_VARIABLES_CHARACTER_SET = "SHOW VARIABLES WHERE Variable_name IN ('character_set_server','collation_server')";
    private static final String SQL_SHOW_SESSION_VARIABLE_SSL_VERSION = "SHOW SESSION STATUS LIKE 'Ssl_version'";

    private final Map<String, String> originalSystemProperties = new HashMap<>();

    MySqlJdbcContext(MySqlEngineConfig config) {
        this.config = config.getConfig(); // must be set before most methods are used

        // 设置jdbc连接，客户端编码UTF-8
        // per https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-charsets.html
        boolean useSSL = sslModeEnabled();
        //|| x.equals(MySqlEngineConfig.DATABASE_HISTORY.name())
        Configuration jdbcConfig = this.config
                .filter(x -> !(x.startsWith(DatabaseHistory.CONFIGURATION_FIELD_PREFIX_STRING)))
                .edit()
                .withDefault(MySqlEngineConfig.PORT, MySqlEngineConfig.PORT.defaultValue())
                .withDefault("database.useCursorFetch", config.useCursorFetch())
                .build()
                .subset("database.", true);

        Configuration.Builder jdbcConfigBuilder = jdbcConfig
                .edit()
                .with("connectTimeout", Long.toString(config.getConnectionTimeout().toMillis()))
                .with("useSSL", Boolean.toString(useSSL));

        final String legacyDateTime = jdbcConfig.getString(JDBC_PROPERTY_LEGACY_DATETIME);
        jdbcConfigBuilder.with(JDBC_PROPERTY_LEGACY_DATETIME, "false");

        jdbcConfig = jdbcConfigBuilder.build();
        String driverClassName = jdbcConfig.getString(MySqlEngineConfig.JDBC_DRIVER);
        this.jdbcConnection = new JdbcConnection(jdbcConfig,
                JdbcConnection.patternBasedFactory(MYSQL_CONNECTION_URL, driverClassName, getClass().getClassLoader()), "`", "`");
    }

    public Configuration config() {
        return config;
    }

    public JdbcConnection jdbc() {
        return jdbcConnection;
    }

    public Logger logger() {
        return logger;
    }

    public String username() {
        return config.getString(MySqlEngineConfig.USER);
    }

    public String password() {
        return config.getString(MySqlEngineConfig.PASSWORD);
    }

    public String hostname() {
        return config.getString(MySqlEngineConfig.HOSTNAME);
    }

    public int port() {
        return config.getInteger(MySqlEngineConfig.PORT);
    }

    public SecureConnectionMode sslMode() {
        String mode = config.getString(MySqlEngineConfig.SSL_MODE);
        return SecureConnectionMode.parse(mode);
    }

    public boolean sslModeEnabled() {
        return sslMode() != SecureConnectionMode.DISABLED;
    }

    public EventProcessingFailureHandlingMode eventProcessingFailureHandlingMode() {
        String mode = config.getString(CommonEngineConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);
        if (mode == null) {
            mode = config.getString(MySqlEngineConfig.EVENT_DESERIALIZATION_FAILURE_HANDLING_MODE);
        }
        return EventProcessingFailureHandlingMode.parse(mode);
    }

    public EventProcessingFailureHandlingMode inconsistentSchemaHandlingMode() {
        String mode = config.getString(MySqlEngineConfig.INCONSISTENT_SCHEMA_HANDLING_MODE);
        return EventProcessingFailureHandlingMode.parse(mode);
    }

    public void start() {
        if (sslModeEnabled()) {
            originalSystemProperties.clear();
            // Set the System properties for SSL for the MySQL driver ...
            setSystemProperty("javax.net.ssl.keyStore", MySqlEngineConfig.SSL_KEYSTORE, true);
            setSystemProperty("javax.net.ssl.keyStorePassword", MySqlEngineConfig.SSL_KEYSTORE_PASSWORD, false);
            setSystemProperty("javax.net.ssl.trustStore", MySqlEngineConfig.SSL_TRUSTSTORE, true);
            setSystemProperty("javax.net.ssl.trustStorePassword", MySqlEngineConfig.SSL_TRUSTSTORE_PASSWORD, false);
        }
    }

    public void shutdown() {
        try {
            jdbcConnection.close();
        } catch (SQLException e) {
            logger.error("Unexpected error shutting down the database connection", e);
        } finally {
            // Reset the system properties to their original value ...
            originalSystemProperties.forEach((name, value) -> {
                if (value != null) {
                    System.setProperty(name, value);
                } else {
                    System.clearProperty(name);
                }
            });
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    /*
     * @Description:是否支持Gtid模式
     * @Param:
     * @return: boolean
     * @Author: miaoneng
     * @Date: 2021/11/29 11:42
     */
    public boolean isGtidModeEnabled() {
        AtomicReference<String> mode = new AtomicReference<String>("off");
        try {
            jdbc().query("SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'", rs -> {
                if (rs.next()) {
                    mode.set(rs.getString(2));
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }

        return !"OFF".equalsIgnoreCase(mode.get());
    }

    /*
     * @Description:查找mysql已经执行的GTID
     * @Param:null
     * @return: java.lang.String 表示当前已经执行的GTID的字符串
     * @Author: miaoneng
     * @Date: 2021/10/22 16:39
     */
    public String knownGtidSet() {
        AtomicReference<String> gtidSetStr = new AtomicReference<String>();
        try {
            jdbcConnection.query("SHOW MASTER STATUS", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 4) {
                    gtidSetStr.set(rs.getString(5)); // GTID set, may be null, blank, or contain a GTID set
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }

        String result = gtidSetStr.get();
        return result != null ? result : "";
    }

    /**
     * Determine the difference between two sets.
     *
     * @return a subtraction of two GTID sets; never null
     */
    public GtidSet subtractGtidSet(GtidSet set1, GtidSet set2) {
        try {
            return jdbcConnection.prepareQueryAndMap("SELECT GTID_SUBTRACT(?, ?)",
                    ps -> {
                        ps.setString(1, set1.toString());
                        ps.setString(2, set2.toString());
                    },
                    rs -> {
                        if (rs.next()) {
                            return new GtidSet(rs.getString(1));
                        }
                        return new GtidSet("");
                    });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at GTID mode: ", e);
        }
    }

    /**
     * Get the purged GTID values from MySQL (gtid_purged value)
     *
     * @return A GTID set; may be empty if not using GTIDs or none have been purged yet
     */
    public GtidSet purgedGtidSet() {
        AtomicReference<String> gtidSetStr = new AtomicReference<String>();
        try {
            jdbcConnection.query("SELECT @@global.gtid_purged", rs -> {
                if (rs.next() && rs.getMetaData().getColumnCount() > 0) {
                    gtidSetStr.set(rs.getString(1)); // GTID set, may be null, blank, or contain a GTID set
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at gtid_purged variable: ", e);
        }

        String result = gtidSetStr.get();
        if (result == null) {
            result = "";
        }

        return new GtidSet(result);
    }

    /**
     * Determine if the current user has the named privilege. Note that if the user has the "ALL" privilege this method
     * returns {@code true}.
     *
     * @param grantName the name of the MySQL privilege; may not be null
     * @return {@code true} if the user has the named privilege, or {@code false} otherwise
     */
    public boolean userHasPrivileges(String grantName) {
        AtomicBoolean result = new AtomicBoolean(false);
        try {
            jdbcConnection.query("SHOW GRANTS FOR CURRENT_USER", rs -> {
                while (rs.next()) {
                    String grants = rs.getString(1);
                    logger.debug(grants);
                    if (grants == null) {
                        return;
                    }
                    grants = grants.toUpperCase();
                    if (grants.contains("ALL") || grants.contains(grantName.toUpperCase())) {
                        result.set(true);
                    }
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Unexpected error while connecting to MySQL and looking at privileges for current user: ", e);
        }
        return result.get();
    }

    public String connectionString() {
        return jdbcConnection.connectionString(MYSQL_CONNECTION_URL);
    }

    /**
     * Read the MySQL charset-related system variables.
     *
     * @return the system variables that are related to server character sets; never null
     */
    protected Map<String, String> readMySqlCharsetSystemVariables() {
        // Read the system variables from the MySQL instance and get the current database name ...
        logger.debug("Reading MySQL charset-related system variables before parsing DDL history.");
        return querySystemVariables(SQL_SHOW_SYSTEM_VARIABLES_CHARACTER_SET);
    }

    /**
     * Read the MySQL system variables.
     *
     * @return the system variables that are related to server character sets; never null
     */
    public Map<String, String> readMySqlSystemVariables() {
        // Read the system variables from the MySQL instance and get the current database name ...
        logger.debug("Reading MySQL system variables");
        return querySystemVariables(SQL_SHOW_SYSTEM_VARIABLES);
    }

    private Map<String, String> querySystemVariables(String statement) {
        Map<String, String> variables = new HashMap<>();
        try {
            start();
            jdbcConnection.connect().query(statement, rs -> {
                while (rs.next()) {
                    String varName = rs.getString(1);
                    String value = rs.getString(2);
                    if (varName != null && value != null) {
                        variables.put(varName, value);
                        logger.debug("\t{} = {}",
                                Strings.pad(varName, 45, ' '),
                                Strings.pad(value, 45, ' '));
                    }
                }
            });
        } catch (SQLException e) {
            throw new ConnectException("Error reading MySQL variables: " + e.getMessage(), e);
        }

        return variables;
    }

    /**
     * Read the MySQL default character sets for exisiting databases.
     *
     * @return the map of database names with their default character sets; never null
     */
    protected Map<String, DatabaseLocales> readDatabaseCollations() {
        logger.debug("Reading default database charsets");
        try {
            start();
            return jdbcConnection.connect().queryAndMap("SELECT schema_name, default_character_set_name, default_collation_name FROM information_schema.schemata", rs -> {
                final Map<String, DatabaseLocales> charsets = new HashMap<>();
                while (rs.next()) {
                    String dbName = rs.getString(1);
                    String charset = rs.getString(2);
                    String collation = rs.getString(3);
                    if (dbName != null && (charset != null || collation != null)) {
                        charsets.put(dbName, new DatabaseLocales(charset, collation));
                        logger.debug("\t{} = {}, {}",
                                Strings.pad(dbName, 45, ' '),
                                Strings.pad(charset, 45, ' '),
                                Strings.pad(collation, 45, ' '));
                    }
                }
                return charsets;
            });
        } catch (SQLException e) {
            throw new CdcException("Error reading default database charsets: " + e.getMessage(), e);
        }
    }

    protected String setStatementFor(Map<String, String> variables) {
        StringBuilder sb = new StringBuilder("SET ");
        boolean first = true;
        List<String> varNames = new ArrayList<>(variables.keySet());
        Collections.sort(varNames);
        for (String varName : varNames) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(varName).append("=");
            String value = variables.get(varName);
            if (value == null) {
                value = "";
            }
            if (value.contains(",") || value.contains(";")) {
                value = "'" + value + "'";
            }
            sb.append(value);
        }
        return sb.append(";").toString();
    }

    protected void setSystemProperty(String property, Field field, boolean showValueInError) {
        String value = config.getString(field);
        if (value != null) {
            value = value.trim();
            String existingValue = System.getProperty(property);
            if (existingValue == null) {
                // There was no existing property ...
                String existing = System.setProperty(property, value);
                originalSystemProperties.put(property, existing); // the existing value may be null
            } else {
                existingValue = existingValue.trim();
                if (!existingValue.equalsIgnoreCase(value)) {
                    // There was an existing property, and the value is different ...
                    String msg = "System or JVM property '" + property + "' is already defined, but the configuration property '"
                            + field.name()
                            + "' defines a different value";
                    if (showValueInError) {
                        msg = "System or JVM property '" + property + "' is already defined as " + existingValue
                                + ", but the configuration property '" + field.name() + "' defines a different value '" + value + "'";
                    }
                    throw new ConnectException(msg);
                }
                // Otherwise, there was an existing property, and the value is exactly the same (so do nothing!)
            }
        }
    }

    /**
     * Read the Ssl Version session variable.
     *
     * @return the session variables that are related to sessions ssl version
     */
    public String getSessionVariableForSslVersion() {
        final String SSL_VERSION = "Ssl_version";
        logger.debug("Reading MySQL Session variable for Ssl Version");
        Map<String, String> sessionVariables = querySystemVariables(SQL_SHOW_SESSION_VARIABLE_SSL_VERSION);
        if (!sessionVariables.isEmpty() && sessionVariables.containsKey(SSL_VERSION)) {
            return sessionVariables.get(SSL_VERSION);
        }
        return null;
    }

    public static class DatabaseLocales {
        private final String charset;
        private final String collation;

        public DatabaseLocales(String charset, String collation) {
            this.charset = charset;
            this.collation = collation;
        }

        public void appendToDdlStatement(String dbName, StringBuilder ddl) {
            if (charset != null) {
                logger.debug("Setting default charset '{}' for database '{}'", charset, dbName);
                ddl.append(" CHARSET ").append(charset);
            } else {
                logger.info("Default database charset for '{}' not found", dbName);
            }
            if (collation != null) {
                logger.debug("Setting default collation '{}' for database '{}'", collation, dbName);
                ddl.append(" COLLATE ").append(collation);
            } else {
                logger.info("Default database collation for '{}' not found", dbName);
            }
        }
    }
}
