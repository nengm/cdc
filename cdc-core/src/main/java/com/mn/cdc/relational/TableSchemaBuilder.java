package com.mn.cdc.relational;

import com.mn.cdc.data.Envelope;
import com.mn.cdc.data.SchemaUtil;
import com.mn.cdc.exception.DataException;
import com.mn.cdc.mapping.ColumnMapper;
import com.mn.cdc.mapping.ColumnMappers;
import com.mn.cdc.structure.Field;
import com.mn.cdc.structure.Schema;
import com.mn.cdc.structure.SchemaBuilder;
import com.mn.cdc.structure.Struct;
import com.mn.cdc.util.SchemaNameAdjuster;
import com.mn.cdc.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @program:cdc-master
 * @description 为table构造TableSchema
 * @author:miaoneng
 * @create:2021-09-09 22:39
 **/
public class TableSchemaBuilder {
    private final static Logger logger = LoggerFactory.getLogger(TableSchemaBuilder.class);

    private final SchemaNameAdjuster schemaNameAdjuster;
    private final ValueConverterProvider valueConverterProvider;
    Schema sourceInfoSchema;
    /**
     * Create a new instance of the builder.
     *
     * @param valueConverterProvider
     *            the provider for obtaining {@link ValueConverter}s and
     *            {@link SchemaBuilder}s; may not be
     *            null
     * @param schemaNameAdjuster
     *            the adjuster for schema names; may not be null
     */
    public TableSchemaBuilder(ValueConverterProvider valueConverterProvider, SchemaNameAdjuster schemaNameAdjuster, Schema sourceInfoSchema) {
        this.schemaNameAdjuster = schemaNameAdjuster;
        this.valueConverterProvider = valueConverterProvider;
        this.sourceInfoSchema = sourceInfoSchema;
    }

    public TableSchema create(String schemaPrefix, String envelopSchemaName, Table table, Predicate<ColumnId> filter, ColumnMappers mappers) {
        if (schemaPrefix == null)
            schemaPrefix = "";
        // Build the schemas ...
        final TableId tableId = table.id();
        final String tableIdStr = tableSchemaName(tableId);
        final String schemaNamePrefix = schemaPrefix + tableIdStr;
        logger.debug("Mapping table '{}' to schemas under '{}'", tableId, schemaNamePrefix);
        //值结构生成器
        SchemaBuilder valSchemaBuilder = SchemaBuilder.struct().name(schemaNameAdjuster.adjust(schemaNamePrefix + ".Value"));

        //主键结构生成器
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct().name(schemaNameAdjuster.adjust(schemaNamePrefix + ".Key"));
        AtomicBoolean hasPrimaryKey = new AtomicBoolean(false);
        table.columns().forEach(column -> {
            if (table.isPrimaryKeyColumn(column.name())) {
                // The column is part of the primary key, so ALWAYS add it to
                // the PK schema ...
                addField(keySchemaBuilder, column, null);
                hasPrimaryKey.set(true);
            }
            if (filter == null || filter.test(new ColumnId(tableId, column.name()))) {
                // Add the column to the value schema only if the column has not
                // been filtered ...
                ColumnMapper mapper = mappers == null ? null : mappers.mapperFor(tableId, column);
                addField(valSchemaBuilder, column, mapper);
            }
        });
        Schema valSchema = valSchemaBuilder.optional().build();
        Schema keySchema = hasPrimaryKey.get() ? keySchemaBuilder.build() : null;

        if (logger.isDebugEnabled()) {
            logger.debug("Mapped primary key for table '{}' to schema: {}", tableId, SchemaUtil.asDetailedString(keySchema));
            logger.debug("Mapped columns for table '{}' to schema: {}", tableId, SchemaUtil.asDetailedString(valSchema));
        }

        //消息体
        Envelope envelope = Envelope.defineSchema()
                .withName(schemaNameAdjuster.adjust(envelopSchemaName))
                .withRecord(valSchema)
                .withSource(sourceInfoSchema)
                .build();

        //主键转换器
        Function<Object[], Struct> keyGenerator = createKeyGenerator(keySchema, tableId, table.primaryKeyColumns());

        //值转换器
        Function<Object[], Struct> valueGenerator = createValueGenerator(valSchema, tableId, table.columns(), filter, mappers);

        // 表结构定义
        return new TableSchema(tableId, keySchema, keyGenerator, envelope, valSchema,valueGenerator);
    }
    protected Function<Object[], Struct> createKeyGenerator(Schema schema, TableId columnSetName, List<Column> columns) {
        if (schema != null) {
            int[] recordIndexes = indexesForColumns(columns);
            Field[] fields = fieldsForColumns(schema, columns);
            int numFields = recordIndexes.length;
            ValueConverter[] converters = convertersForColumns(schema, columnSetName, columns, null,null);
            return (row) -> {
                Struct result = new Struct(schema);
                for (int i = 0; i != numFields; ++i) {
                    Object value = row[recordIndexes[i]];
                    ValueConverter converter = converters[i];
                    if (converter != null) {
                        // A component of primary key must be not-null.
                        // It is possible for some databases and values (MySQL and all-zero datetime)
                        // to be reported as null by JDBC or streaming reader.
                        // It thus makes sense to convert them to a sensible default replacement value.
                        value = converter.convert(value);
                        try {
                            result.put(fields[i], value);
                        }
                        catch (DataException e) {
                            Column col = columns.get(i);
                            logger.error("Failed to properly convert key value for '{}.{}' of type {} for row {}:",
                                    columnSetName, col.name(), col.typeName(), row, e);
                        }
                    }
                }
                return result;
            };
        }
        return null;
    }
    /**
     * Creates the function that produces a Kafka Connect value object for a row of data.
     *
     * @param schema the Kafka Connect schema for the value; may be null if there is no known schema, in which case the generator
     *            will be null
     * @param tableId the table identifier; may not be null
     * @param columns the column definitions for the table that defines the row; may not be null
     * @param filter the filter that specifies whether columns in the table should be included; may be null if all columns
     *            are to be included
     * @param mappers the mapping functions for columns; may be null if none of the columns are to be mapped to different values
     * @return the value-generating function, or null if there is no value schema
     */
    protected Function<Object[], Struct> createValueGenerator(Schema schema, TableId tableId, List<Column> columns,
                                                              Predicate<ColumnId> filter, ColumnMappers mappers) {
        if (schema != null) {
            int[] recordIndexes = indexesForColumns(columns);
            Field[] fields = fieldsForColumns(schema, columns);
            int numFields = recordIndexes.length;
            ValueConverter[] converters = convertersForColumns(schema, tableId, columns, filter, mappers);
            return (row) -> {
                Struct result = new Struct(schema);
                for (int i = 0; i != numFields; ++i) {
                    Object value = row[recordIndexes[i]];

                    ValueConverter converter = converters[i];

                    if (converter != null) {
                        logger.trace("converter for value object: *** {} ***", converter);
                    }
                    else {
                        logger.trace("converter is null...");
                    }

                    if (converter != null) {
                        try {
                            value = converter.convert(value);
                            result.put(fields[i], value);
                        }
                        catch (DataException | IllegalArgumentException e) {
                            Column col = columns.get(i);
                            logger.error("Failed to properly convert data value for '{}.{}' of type {} for row {}:",
                                    tableId, col.name(), col.typeName(), row, e);
                        }
                        catch (final Exception e) {
                            Column col = columns.get(i);
                            logger.error("Failed to properly convert data value for '{}.{}' of type {} for row {}:",
                                    tableId, col.name(), col.typeName(), row, e);
                        }
                    }
                }
                return result;
            };
        }
        return null;
    }
    protected int[] indexesForColumns(List<Column> columns) {
        int[] recordIndexes = new int[columns.size()];
        AtomicInteger i = new AtomicInteger(0);
        columns.forEach(column -> {
            recordIndexes[i.getAndIncrement()] = column.position() - 1; // position is 1-based, indexes 0-based
        });
        return recordIndexes;
    }

    protected Field[] fieldsForColumns(Schema schema, List<Column> columns) {
        Field[] fields = new Field[columns.size()];
        AtomicInteger i = new AtomicInteger(0);
        columns.forEach(column -> {
            Field field = schema.field(column.name()); // may be null if the
            // field is unused ...
            fields[i.getAndIncrement()] = field;
        });
        return fields;
    }

    /**
     * 返回表结构名称
     */
    private String tableSchemaName(TableId tableId) {
        if (Strings.isNullOrEmpty(tableId.catalog())) {
            if (Strings.isNullOrEmpty(tableId.schema())) {
                return tableId.table();
            } else {
                return tableId.schema() + "." + tableId.table();
            }
        } else if (Strings.isNullOrEmpty(tableId.schema())) {
            return tableId.catalog() + "." + tableId.table();
        }
        // When both catalog and schema is present then only schema is used
        else {
            return tableId.schema() + "." + tableId.table();
        }
    }

    /**
     * Add to the supplied {@link SchemaBuilder} a field for the column with the
     * given information.
     *
     * @param builder
     *            the schema builder; never null
     * @param column
     *            the column definition
     * @param mapper
     *            the mapping function for the column; may be null if the
     *            columns is not to be mapped to different values
     */
    protected void addField(SchemaBuilder builder, Column column, ColumnMapper mapper) {
        SchemaBuilder fieldBuilder = valueConverterProvider.schemaBuilder(column);
        if (fieldBuilder != null) {
            if (mapper != null) {
                // Let the mapper add properties to the schema ...
                //使用隐射改变一些列的值，可以不使用，默认就不改变任何值，其实可以去掉
                mapper.alterFieldSchema(column, fieldBuilder);
            }
            if (column.isOptional())
                fieldBuilder.optional();

            // if the default value is provided
            if (column.hasDefaultValue()) {
                fieldBuilder.defaultValue(column.defaultValue());
            }

            builder.field(column.name(), fieldBuilder.build());
            if (logger.isDebugEnabled()) {
                logger.debug("- field '{}' ({}{}) from column {}", column.name(), builder.isOptional() ? "OPTIONAL " : "",
                        fieldBuilder.type(),
                        column);
            }
        } else {
            logger.warn("Unexpected JDBC type '{}' for column '{}' that will be ignored", column.jdbcType(), column.name());
        }
    }


    /**
     * Obtain the array of converters for each column in a row. A converter
     * might be null if the column is not be included in
     * the records.
     *
     * @param schema
     *            the schema; may not be null
     * @param tableId
     *            the identifier of the table that contains the columns
     * @param columns
     *            the columns in the row; may not be null
     * @param filter
     *            the filter that specifies whether columns in the table should
     *            be included; may be null if all columns
     *            are to be included
     * @param mappers
     *            the mapping functions for columns; may be null if none of the
     *            columns are to be mapped to different values
     * @return the converters for each column in the rows; never null
     */
    protected ValueConverter[] convertersForColumns(Schema schema, TableId tableId, List<Column> columns,
                                                    Predicate<ColumnId> filter, ColumnMappers mappers) {

        ValueConverter[] converters = new ValueConverter[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);

            if (filter != null && !filter.test(new ColumnId(tableId, column.name()))) {
                continue;
            }

            ValueConverter converter = createValueConverterFor(column, schema.field(column.name()));

            //是否需要改名等，目前我们没这需要
            converter = wrapInMappingConverterIfNeeded(mappers, tableId, column, converter);

            if (converter == null) {
                logger.warn(
                        "No converter found for column {}.{} of type {}. The column will not be part of change events for that table.",
                        tableId, column.name(), column.typeName());
            }

            // may be null if no converter found
            converters[i] = converter;
        }

        return converters;
    }

    private ValueConverter wrapInMappingConverterIfNeeded(ColumnMappers mappers, TableId tableId, Column column, ValueConverter converter) {
        if (mappers == null || converter == null) {
            return converter;
        }

        ValueConverter mappingConverter = mappers.mappingConverterFor(tableId, column);
        if (mappingConverter == null) {
            return converter;
        }

        return (value) -> {
            if (value != null) {
                value = converter.convert(value);
            }

            return mappingConverter.convert(value);
        };
    }

    /**
     * Create a {@link ValueConverter} that can be used to convert row values
     * for the given column into the Kafka Connect value
     * object described by the {@link Field field definition}. This uses the
     * supplied {@link ValueConverterProvider} object.
     *
     * @param column
     *            the column describing the input values; never null
     * @param fieldDefn
     *            the definition for the field in a Kafka Connect {@link Schema}
     *            describing the output of the function;
     *            never null
     * @return the value conversion function; may not be null
     */
    protected ValueConverter createValueConverterFor(Column column, Field fieldDefn) {
        return valueConverterProvider.converter(column, fieldDefn);
    }
}
