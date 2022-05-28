package com.mn.cdc.relational;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @program:cdc-master
 * @description 不需要编辑table
 * @author:miaoneng
 * @create:2021-09-10 15:23
 **/
public class NoOpTableEditorImpl implements TableEditor {
    private TableId id;
    private boolean uniqueValues = false;
    private String defaultCharsetName;

    protected NoOpTableEditorImpl() {
    }

    @Override
    public TableId tableId() {
        return id;
    }

    @Override
    public TableEditor tableId(TableId id) {
        this.id = id;
        return this;
    }

    @Override
    public List<Column> columns() {
        return Collections.emptyList();
    }

    @Override
    public Column columnWithName(String name) {
        return null;
    }

    protected boolean hasColumnWithName(String name) {
        return false;
    }

    @Override
    public List<String> primaryKeyColumnNames() {
        return Collections.emptyList();
    }

    @Override
    public TableEditor addColumns(Column... columns) {
        return this;
    }

    @Override
    public TableEditor addColumns(Iterable<Column> columns) {
        return this;
    }

    @Override
    public TableEditor setColumns(Column... columns) {
        return this;
    }

    @Override
    public TableEditor setColumns(Iterable<Column> columns) {
        return this;
    }

    @Override
    public TableEditor setPrimaryKeyNames(String... pkColumnNames) {
        return this;
    }

    @Override
    public TableEditor setPrimaryKeyNames(List<String> pkColumnNames) {
        return this;
    }

    @Override
    public TableEditor setUniqueValues() {
        this.uniqueValues = true;
        return this;
    }

    @Override
    public boolean hasUniqueValues() {
        return uniqueValues;
    }

    @Override
    public TableEditor setDefaultCharsetName(String charsetName) {
        this.defaultCharsetName = charsetName;
        return this;
    }

    @Override
    public boolean hasDefaultCharsetName() {
        return this.defaultCharsetName != null && !this.defaultCharsetName.trim().isEmpty();
    }

    @Override
    public TableEditor removeColumn(String columnName) {
        return this;
    }

    @Override
    public TableEditor updateColumn(Column column) {
        return this;
    }

    @Override
    public TableEditor reorderColumn(String columnName, String afterColumnName) {
        return this;
    }

    @Override
    public TableEditor renameColumn(String existingName, String newName) {
        return this;
    }

    @Override
    public String toString() {
        return create().toString();
    }

    @Override
    public Table create() {
        if (id == null) {
            throw new IllegalStateException("Unable to create a table from an editor that has no table ID");
        }
        List<Column> columns = new ArrayList<>();
        return new TableImpl(id, columns, primaryKeyColumnNames(), defaultCharsetName);
    }
}