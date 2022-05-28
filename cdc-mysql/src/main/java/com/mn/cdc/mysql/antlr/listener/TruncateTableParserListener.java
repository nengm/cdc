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

/**
 * Parser listener that is parsing MySQL TRUNCATE TABLE statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class TruncateTableParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;

    public TruncateTableParserListener(MySqlAntlrDdlParser parser) {
        this.parser = parser;
    }

    @Override
    public void enterTruncateTable(MySqlParser.TruncateTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        // Be aware the legacy parser is not signaling truncate events
        parser.signalTruncateTable(tableId, ctx);
        super.enterTruncateTable(ctx);
    }
}
