/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.mn.cdc.relational.ddl;


import com.mn.cdc.relational.SystemVariables;
import com.mn.cdc.relational.Tables;
import com.mn.cdc.text.ParsingException;

/**
 * A parser interface for DDL statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public interface DdlParser {

    /**
     * Examine the supplied string containing DDL statements, and apply those statements to the specified
     * database table definitions.
     *
     * @param ddlContent     the stream of tokens containing the DDL statements; may not be null
     * @param databaseTables the database's table definitions, which should be used by this method to create, change, or remove
     *                       tables as defined in the DDL content; may not be null
     * @throws ParsingException if there is a problem parsing the supplied content
     */
    void parse(String ddlContent, Tables databaseTables);

    void setCurrentDatabase(String databaseName);

    /**
     * Set the name of the current schema.
     *
     * @param schemaName the name of the current schema; may be null
     */
    void setCurrentSchema(String schemaName);

    DdlChanges getDdlChanges();

    /**
     * The token used to terminate a DDL statement.
     *
     * @return the terminating token; never null
     */
    String terminator();

    SystemVariables systemVariables();
}
