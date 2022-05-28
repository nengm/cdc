package com.mn.cdc.relational.history;

import com.mn.cdc.document.Array;
import com.mn.cdc.document.Document;
import com.mn.cdc.document.Value;
import com.mn.cdc.relational.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-09-13 10:53
 **/
public class TableChanges implements Iterable<TableChanges.TableChange> {
    private final List<TableChange> changes;

    public TableChanges() {
        this.changes = new ArrayList<>();
    }

    public static TableChanges fromArray(Array array) {
        TableChanges tableChanges = new TableChanges();

        for (Array.Entry entry : array) {
            TableChange change = TableChange.fromDocument(entry.getValue().asDocument());

            if (change.getType() == TableChangeType.CREATE) {
                tableChanges.create(change.table);
            }
            else if (change.getType() == TableChangeType.ALTER) {
                tableChanges.alter(change.table);
            }
        }

        return tableChanges;
    }

    public TableChanges create(Table table) {
        changes.add(new TableChange(TableChangeType.CREATE, table));
        return this;
    }

    public TableChanges alter(Table table) {
        changes.add(new TableChange(TableChangeType.ALTER, table));
        return this;
    }
    public TableChanges drop(Table table) {
        changes.add(new TableChange(TableChangeType.DROP, table));
        return this;
    }
    @Override
    public Iterator<TableChange> iterator() {
        return changes.iterator();
    }

    public Array toArray() {
        List<Value> values = changes.stream()
                .map(TableChange::toDocument)
                .map(Value::create)
                .collect(Collectors.toList());

        return Array.create(values);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + changes.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableChanges other = (TableChanges) obj;

        return changes.equals(other.changes);
    }

    @Override
    public String toString() {
        return "TableChanges [changes=" + changes + "]";
    }

    public static class TableChange {

        private final TableChangeType type;
        private final TableId id;
        private final Table table;

        public TableChange(TableChangeType type, Table table) {
            this.type = type;
            this.table = table;
            this.id = table.id();
        }

        public static TableChange fromDocument(Document document) {
            TableChangeType type = TableChangeType.valueOf(document.getString("type"));
            TableId id = TableId.parse(document.getString("id"));
            Table table = null;

            if (type == TableChangeType.CREATE || type == TableChangeType.ALTER) {
                table = fromDocument(id, document.getDocument("table"));
            }

            return new TableChange(type, table);
        }

        public TableChangeType getType() {
            return type;
        }

        public TableId getId() {
            return id;
        }

        public Table getTable() {
            return table;
        }

        public Document toDocument() {
            Document document = Document.create();

            document.setString("type", type.name());
            document.setString("id", id.toString());
            document.setDocument("table", toDocument(table));
            return document;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + id.hashCode();
            result = prime * result + ((table == null) ? 0 : table.hashCode());
            result = prime * result + type.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TableChange other = (TableChange) obj;
            if (!id.equals(other.id))
                return false;
            if (table == null) {
                if (other.table != null)
                    return false;
            } else if (!table.equals(other.table))
                return false;
            if (type != other.type)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "TableChange [type=" + type + ", id=" + id + ", table=" + table + "]";
        }

        private Document toDocument(Table table) {
            Document document = Document.create();

            document.set("defaultCharsetName", table.defaultCharsetName());
            document.set("primaryKeyColumnNames", Array.create(table.primaryKeyColumnNames()));

            List<Document> columns = table.columns()
                    .stream()
                    .map(this::toDocument)
                    .collect(Collectors.toList());

            document.setArray("columns", Array.create(columns));

            return document;
        }

        private Document toDocument(Column column) {
            Document document = Document.create();

            document.setString("name", column.name());
            document.setNumber("jdbcType", column.jdbcType());

            if (column.nativeType() != Column.UNSET_INT_VALUE) {
                document.setNumber("nativeType", column.nativeType());
            }

            document.setString("typeName", column.typeName());
            document.setString("typeExpression", column.typeExpression());
            document.setString("charsetName", column.charsetName());

            if (column.length() != Column.UNSET_INT_VALUE) {
                document.setNumber("length", column.length());
            }

            column.scale().ifPresent(s -> document.setNumber("scale", s));

            document.setNumber("position", column.position());
            document.setBoolean("optional", column.isOptional());
            document.setBoolean("autoIncremented", column.isAutoIncremented());
            document.setBoolean("generated", column.isGenerated());

            return document;
        }

        private static Table fromDocument(TableId id, Document document) {
            TableEditor editor = Table.editor()
                    .tableId(id)
                    .setDefaultCharsetName(document.getString("defaultCharsetName"));

            document.getArray("columns")
                    .streamValues()
                    .map(Value::asDocument)
                    .map(v -> {
                        ColumnEditor columnEditor = Column.editor()
                                .name(v.getString("name"))
                                .jdbcType(v.getInteger("jdbcType"));

                        Integer nativeType = v.getInteger("nativeType");
                        if (nativeType != null) {
                            columnEditor.nativeType(nativeType);
                        }

                        columnEditor.type(v.getString("typeName"), v.getString("typeExpression"))
                                .charsetName(v.getString("charsetName"));

                        Integer length = v.getInteger("length");
                        if (length != null) {
                            columnEditor.length(length);
                        }

                        Integer scale = v.getInteger("scale");
                        if (scale != null) {
                            columnEditor.scale(scale);
                        }

                        columnEditor.position(v.getInteger("position"))
                                .optional(v.getBoolean("optional"))
                                .autoIncremented(v.getBoolean("autoIncremented"))
                                .generated(v.getBoolean("generated"));

                        return columnEditor.create();
                    })
                    .forEach(editor::addColumn);

            editor.setPrimaryKeyNames(document.getArray("primaryKeyColumnNames")
                    .streamValues()
                    .map(Value::asString)
                    .collect(Collectors.toList()));

            return editor.create();
        }
    }

    public enum TableChangeType {
        CREATE,
        ALTER,
        DROP;
    }

    /**
     * The interface that defines conversion of {@code TableChanges} into a serialized format for
     * persistent storage or delivering as a message.
     *
     * @author Jiri Pechanec
     *
     * @param <T> target type
     */
    public static interface TableChangesSerializer<T> {

        T serialize(TableChanges tableChanges);

        /**
         * @param useCatalogBeforeSchema true if the parsed string contains only 2 items and the first should be used as
         * the catalog and the second as the table name, or false if the first should be used as the schema and the
         * second as the table name
         */
        TableChanges deserialize(T data, boolean useCatalogBeforeSchema);
    }
}
