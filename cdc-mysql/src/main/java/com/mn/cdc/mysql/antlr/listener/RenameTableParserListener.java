/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.mn.cdc.mysql.antlr.listener;

import com.mn.cdc.d4.MySqlParser;
import com.mn.cdc.d4.MySqlParserBaseListener;
import com.mn.cdc.mysql.antlr.MySqlAntlrDdlParser;
import com.mn.cdc.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parser listener that is parsing MySQL RENAME TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class RenameTableParserListener extends MySqlParserBaseListener {

    private final static Logger LOG = LoggerFactory.getLogger(RenameTableParserListener.class);

    private final MySqlAntlrDdlParser parser;

    public RenameTableParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterRenameTableClause(MySqlParser.RenameTableClauseContext ctx) {
        TableId oldTable = parser.parseQualifiedTableId(ctx.tableName(0).fullId());
        TableId newTable = parser.parseQualifiedTableId(ctx.tableName(1).fullId());
        if (parser.getTableFilter().isIncluded(oldTable) && !parser.getTableFilter().isIncluded(newTable)) {
            LOG.warn("Renaming whitelisted table {} to non-whitelisted table {}, this can lead to schema inconsistency", oldTable, newTable);
        }
        else if (!parser.getTableFilter().isIncluded(oldTable) && parser.getTableFilter().isIncluded(newTable)) {
            LOG.warn("Renaming non-whitelisted table {} to whitelisted table {}, this can lead to schema inconsistency", oldTable, newTable);
        }
        parser.databaseTables().renameTable(oldTable, newTable);
        parser.signalAlterTable(newTable, oldTable, ctx);
        super.enterRenameTableClause(ctx);
    }
}
