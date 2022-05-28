package com.mn.cdc.oracle.listener;

import com.mn.cdc.oracle.entity.EventRowData;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @program:cdc
 * @description
 * @author:miaoneng
 * @create:2022-04-29 16:57
 **/
public class LogParser {
    private static Logger LOG = LoggerFactory.getLogger(LogParser.class);
    public static void parseInsertStmt(
            Insert insert, ArrayList<EventRowData> beforeData, ArrayList<EventRowData> afterData) {
        ArrayList<String> columnLists = new ArrayList<>();
        for (Column column : insert.getColumns()) {
            columnLists.add(cleanString(column.getColumnName()));
        }

        ExpressionList eList = (ExpressionList) insert.getItemsList();
        List<Expression> valueList = eList.getExpressions();
        int i = 0;
        for (String key : columnLists) {
            String value = cleanString(valueList.get(i).toString());
            afterData.add(new EventRowData(key, value, Objects.isNull(value)));
            beforeData.add(new EventRowData(key, null, true));
            i++;
        }
    }
    public static void parseUpdateStmt(
            Update update,
            ArrayList<EventRowData> beforeData,
            ArrayList<EventRowData> afterData,
            String sqlRedo) {
        Iterator<Expression> iterator = update.getExpressions().iterator();
        HashSet<String> columns = new HashSet<>(32);
        for (Column c : update.getColumns()) {
            String value = cleanString(iterator.next().toString());
            String columnName = cleanString(c.getColumnName());
            boolean isNull = Objects.isNull(value) || value.equalsIgnoreCase("= NULL");

            afterData.add(new EventRowData(columnName, isNull ? null : value, isNull));
            columns.add(columnName);
        }

        if (update.getWhere() != null) {
            update.getWhere()
                    .accept(
                            new ExpressionVisitorAdapter() {
                                @Override
                                public void visit(final EqualsTo expr) {
                                    String col = cleanString(expr.getLeftExpression().toString());
                                    String value =
                                            cleanString(expr.getRightExpression().toString());

                                    boolean isNull =
                                            Objects.isNull(value)
                                                    || value.equalsIgnoreCase("= NULL");
                                    beforeData.add(
                                            new EventRowData(col, isNull ? null : value, isNull));
                                    if (!columns.contains(col)) {
                                        afterData.add(
                                                new EventRowData(
                                                        col, isNull ? null : value, isNull));
                                    }
                                }
                            });
        } else {
            LOG.error(
                    "where is null when LogParser parse sqlRedo, sqlRedo = {}, update = {}",
                    sqlRedo,
                    update.toString());
        }
    }
    public static void parseDeleteStmt(
            Delete delete, ArrayList<EventRowData> beforeData, ArrayList<EventRowData> afterData) {
        delete.getWhere()
                .accept(
                        new ExpressionVisitorAdapter() {
                            @Override
                            public void visit(final EqualsTo expr) {
                                String col = cleanString(expr.getLeftExpression().toString());
                                String value = cleanString(expr.getRightExpression().toString());
                                boolean isNull =
                                        Objects.isNull(value) || value.equalsIgnoreCase("= NULL");
                                beforeData.add(
                                        new EventRowData(col, isNull ? null : value, isNull));
                                afterData.add(new EventRowData(col, null, true));
                            }
                        });
    }
    public static String cleanString(String str) {
        if ("NULL".equalsIgnoreCase(str)) {
            return null;
        }

        if (str.startsWith("TIMESTAMP")) {
            str = str.replace("TIMESTAMP ", "");
        }

        if (str.startsWith("'") && str.endsWith("'") && str.length() != 1) {
            str = str.substring(1, str.length() - 1);
        }

        if (str.startsWith("\"") && str.endsWith("\"") && str.length() != 1) {
            str = str.substring(1, str.length() - 1);
        }

        return str.replace("IS NULL", "= NULL").trim();
    }
}
