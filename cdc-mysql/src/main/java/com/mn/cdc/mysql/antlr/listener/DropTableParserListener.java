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
import org.antlr.v4.runtime.misc.Interval;


/**
 * Parser listener that is parsing MySQL DROP TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class DropTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public DropTableParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterDropTable(MySqlParser.DropTableContext ctx) {
        Interval interval = new Interval(ctx.start.getStartIndex(), ctx.tables().start.getStartIndex() - 1);
        String prefix = ctx.start.getInputStream().getText(interval);
        ctx.tables().tableName().forEach(tableNameContext -> {
            TableId tableId = parser.parseQualifiedTableId(tableNameContext.fullId());
            parser.databaseTables().removeTable(tableId);
            parser.signalDropTable(tableId, prefix + tableId.toQuotedString('`')
                    + (ctx.dropType != null ? " " + ctx.dropType.getText() : ""));
        });
        super.enterDropTable(ctx);
    }
}
