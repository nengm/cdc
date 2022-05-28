/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.mn.cdc.mysql.antlr.listener;

import com.mn.cdc.antlr.AntlrDdlParser;
import com.mn.cdc.d4.MySqlParser;
import com.mn.cdc.d4.MySqlParserBaseListener;
import com.mn.cdc.mysql.antlr.MySqlAntlrDdlParser;
import com.mn.cdc.relational.TableEditor;
import com.mn.cdc.relational.TableId;
import com.mn.cdc.text.ParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Parser listener that is parsing MySQL CREATE UNIQUE INDEX statements, that will be used as a primary key
 * if it's not already defined for the table.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class CreateUniqueIndexParserListener extends MySqlParserBaseListener {

    private final static Logger LOG = LoggerFactory.getLogger(AlterTableParserListener.class);

    private final MySqlAntlrDdlParser parser;

    public CreateUniqueIndexParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterCreateIndex(MySqlParser.CreateIndexContext ctx) {
        if (ctx.UNIQUE() != null) {
            TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
            if (!parser.getTableFilter().isIncluded(tableId)) {
                LOG.debug("{} is not monitored, no need to process unique index", tableId);
                return;
            }
            TableEditor tableEditor = parser.databaseTables().editTable(tableId);
            if (tableEditor != null) {
                if (!tableEditor.hasPrimaryKey()) {
                    parser.parsePrimaryIndexColumnNames(ctx.indexColumnNames(), tableEditor);
                    parser.signalCreateIndex(parser.parseName(ctx.uid()), tableId, ctx);
                    parser.databaseTables().overwriteTable(tableEditor.create());
                }
            }
            else {
                throw new ParsingException(null, "Trying to create index on non existing table " + tableId.toString() + "."
                        + "Query: " + AntlrDdlParser.getText(ctx));
            }
        }
        super.enterCreateIndex(ctx);
    }
}
