/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.mn.cdc.mysql.antlr.listener;

import java.util.List;

import com.mn.cdc.antlr.AntlrDdlParser;
import com.mn.cdc.d4.MySqlParser;
import com.mn.cdc.d4.MySqlParserBaseListener;
import com.mn.cdc.mysql.antlr.MySqlAntlrDdlParser;
import com.mn.cdc.relational.Column;
import com.mn.cdc.relational.TableEditor;
import com.mn.cdc.relational.TableId;
import com.mn.cdc.text.ParsingException;
import org.antlr.v4.runtime.tree.ParseTreeListener;


/**
 * Parser listener that is parsing MySQL ALTER VIEW statements.
 *
 * @author Roman Kuchár <kucharrom@gmail.com>.
 */
public class AlterViewParserListener extends MySqlParserBaseListener {

    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;

    private TableEditor tableEditor;
    private ViewSelectedColumnsParserListener selectColumnsListener;

    public AlterViewParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterAlterView(MySqlParser.AlterViewContext ctx) {
        if (!parser.skipViews()) {
            TableId tableId = parser.parseQualifiedTableId(ctx.fullId());

            tableEditor = parser.databaseTables().editTable(tableId);
            if (tableEditor == null) {
                throw new ParsingException(null, "Trying to alter view " + tableId.toString()
                        + ", which does not exist. Query:" + AntlrDdlParser.getText(ctx));
            }
            // alter view will override existing columns for a new one
            tableEditor.columnNames().forEach(tableEditor::removeColumn);
            // create new columns just with specified name for now
            if (ctx.uidList() != null) {
                ctx.uidList().uid().stream().map(parser::parseName).forEach(columnName -> {
                    tableEditor.addColumn(Column.editor().name(columnName).create());
                });
            }
            selectColumnsListener = new ViewSelectedColumnsParserListener(tableEditor, parser);
            listeners.add(selectColumnsListener);
        }
        super.enterAlterView(ctx);
    }

    @Override
    public void exitAlterView(MySqlParser.AlterViewContext ctx) {
        parser.runIfNotNull(() -> {
            tableEditor.addColumns(selectColumnsListener.getSelectedColumns());
            // Make sure that the table's character set has been set ...
            if (!tableEditor.hasDefaultCharsetName()) {
                tableEditor.setDefaultCharsetName(parser.currentDatabaseCharset());
            }
            parser.databaseTables().overwriteTable(tableEditor.create());
            listeners.remove(selectColumnsListener);
        }, tableEditor);
        // signal view even if it was skipped
        parser.signalAlterView(parser.parseQualifiedTableId(ctx.fullId()), null, ctx);
        super.exitAlterView(ctx);
    }
}
