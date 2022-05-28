/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.mn.cdc.mysql.antlr.listener;


import com.mn.cdc.d4.MySqlParser;
import com.mn.cdc.d4.MySqlParserBaseListener;
import com.mn.cdc.mysql.antlr.MySqlAntlrDdlParser;

/**
 * Parser listener that is parsing MySQL DROP DATABASE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DropDatabaseParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public DropDatabaseParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropDatabase(MySqlParser.DropDatabaseContext ctx) {
        String databaseName = parser.parseName(ctx.uid());
        parser.databaseTables().removeTablesForDatabase(databaseName);
        parser.charsetNameForDatabase().remove(databaseName);
        parser.signalDropDatabase(databaseName, ctx);
        super.enterDropDatabase(ctx);
    }
}
