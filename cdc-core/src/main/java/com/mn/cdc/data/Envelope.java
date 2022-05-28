package com.mn.cdc.data;

import com.mn.cdc.structure.Schema;
import com.mn.cdc.structure.SchemaBuilder;
import com.mn.cdc.structure.Struct;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * @program:cdc-master
 * @description 每个消息统一封装
 * @author:miaoneng
 * @create:2021-09-09 14:45
 **/
public class Envelope {
    //操作
    public static enum Operation {
        /**
         * 读取记录当前状态的操作，最常见的是在快照期间，目前我们不使用快照
         */
        READ("r"),
        /**
         * 创建新记录的操作
         */
        CREATE("c"),
        /**
         * 更新现有记录的操作
         */
        UPDATE("u"),
        /**
         * 删除或删除现有记录的操作
         */
        DELETE("d");
        private final String code;

        private Operation(String code) {
            this.code = code;
        }

        public static Operation forCode(String code) {
            for (Operation op : Operation.values()) {
                if (op.code().equalsIgnoreCase(code)) return op;
            }
            return null;
        }

        public String code() {
            return code;
        }
    }

    /**
     * 封装的字段名称
     */
    public static final class FieldName {
        /**
         * 存储操作前记录的状态
         */
        public static final String BEFORE = "before";
        /**
         * 存储操作后记录的状态
         */
        public static final String AFTER = "after";
        /**
         * 操作类型
         */
        public static final String OPERATION = "op";

        /**
         * The {@code origin} field is used to store the information about the source of a record, including the
         * Kafka Connect partition and offset information.
         */
        public static final String SOURCE = "source";

        /**
         * 时间戳
         */
        public static final String TIMESTAMP = "ts_ms";
    }

    /**
     * Flag that specifies whether the {@link FieldName#OPERATION} field is required within the envelope.
     */
    public static final boolean OPERATION_REQUIRED = true;

    /**
     * A builder of an envelope schema.
     */
    public static interface Builder {
        /**
         * Define the {@link Schema} used in the {@link FieldName#BEFORE} and {@link FieldName#AFTER} fields.
         *
         * @param schema the schema of the records, used in the {@link FieldName#BEFORE} and {@link FieldName#AFTER} fields; may
         *            not be null
         * @return this builder so methods can be chained; never null
         */
        default Builder withRecord(Schema schema) {
            return withSchema(schema, FieldName.BEFORE, FieldName.AFTER);
        }

        /**
         * Define the {@link Schema} used in the {@link FieldName#SOURCE} field.
         *
         * @param sourceSchema the schema of the {@link FieldName#SOURCE} field; may not be null
         * @return this builder so methods can be chained; never null
         */
        default Builder withSource(Schema sourceSchema) {
            return withSchema(sourceSchema, FieldName.SOURCE);
        }

        /**
         * Define the {@link Schema} used for an arbitrary field in the envelope.
         *
         * @param fieldNames the names of the fields that this schema should be used with; may not be null
         * @param fieldSchema the schema of the new optional field; may not be null
         * @return this builder so methods can be chained; never null
         */
        Builder withSchema(Schema fieldSchema, String... fieldNames);

        /**
         * Define the name for the schema.
         *
         * @param name the name
         * @return this builder so methods can be chained; never null
         */
        Builder withName(String name);

        /**
         * Define the documentation for the schema.
         *
         * @param doc the documentation
         * @return this builder so methods can be chained; never null
         */
        Builder withDoc(String doc);

        /**
         * Create the message envelope descriptor.
         *
         * @return the envelope schema; never null
         */
        Envelope build();
    }

    public static Builder defineSchema() {
        return new Builder() {
            private final SchemaBuilder builder = SchemaBuilder.struct();
            private final Set<String> missingFields = new HashSet<>();

            @Override
            public Builder withSchema(Schema fieldSchema, String... fieldNames) {
                for (String fieldName : fieldNames) {
                    builder.field(fieldName, fieldSchema);
                }
                return this;
            }

            @Override
            public Builder withName(String name) {
                builder.name(name);
                return this;
            }

            @Override
            public Builder withDoc(String doc) {
                builder.doc(doc);
                return this;
            }

            @Override
            public Envelope build() {
                builder.field(FieldName.OPERATION, OPERATION_REQUIRED ? Schema.STRING_SCHEMA : Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA);
                checkFieldIsDefined(FieldName.OPERATION);
                checkFieldIsDefined(FieldName.BEFORE);
                checkFieldIsDefined(FieldName.AFTER);
                checkFieldIsDefined(FieldName.SOURCE);
                if (!missingFields.isEmpty()) {
                    throw new IllegalStateException("The envelope schema is missing field(s) " + String.join(", ", missingFields));
                }
                return new Envelope(builder.build());
            }

            private void checkFieldIsDefined(String fieldName) {
                if (builder.field(fieldName) == null) {
                    missingFields.add(fieldName);
                }
            }
        };
    }

    private final Schema schema;

    /*
     * @Description:得到描述封装的消息内容的结构
     * @Param:
     * @return: com.mn.cdc.structure.Schema
     * @Author: miaoneng
     * @Date: 2021/11/29 21:28
     */
    public Schema schema() {
        return schema;
    }

    /*
     * @Description:生成create事件
     * @Param: record
     * @param: source
     * @param: timestamp
     * @return: com.mn.cdc.structure.Struct
     * @Author: miaoneng
     * @Date: 2021/11/29 21:29
     */
    public Struct create(Object record, Struct source, Instant timestamp) {
        Struct struct = new Struct(schema);
        struct.put(FieldName.OPERATION, Operation.CREATE.code());
        struct.put(FieldName.AFTER, record);
        if (source != null) struct.put(FieldName.SOURCE, source);
        if (timestamp != null) struct.put(FieldName.TIMESTAMP, timestamp.toEpochMilli());
        return struct;
    }

    /*
     * @Description:删除事件
     * @Param: before
     * @param: source
     * @param: timestamp
     * @return: com.mn.cdc.structure.Struct
     * @Author: miaoneng
     * @Date: 2021/11/30 16:09
     */
    public Struct delete(Object before, Struct source, Instant timestamp) {
        Struct struct = new Struct(schema);
        struct.put(FieldName.OPERATION, Operation.DELETE.code());
        if (before != null) struct.put(FieldName.BEFORE, before);
        if (source != null) struct.put(FieldName.SOURCE, source);
        if (timestamp != null) struct.put(FieldName.TIMESTAMP, timestamp.toEpochMilli());
        return struct;
    }

    /*
     * @Description:更新事件
     * @Param: before
     * @param: after
     * @param: source
     * @param: timestamp
     * @return: com.mn.cdc.structure.Struct
     * @Author: miaoneng
     * @Date: 2021/11/30 16:44
     */
    public Struct update(Object before, Struct after, Struct source, Instant timestamp) {
        Struct struct = new Struct(schema);
        struct.put(FieldName.OPERATION, Operation.UPDATE.code());
        if (before != null) struct.put(FieldName.BEFORE, before);
        struct.put(FieldName.AFTER, after);
        if (source != null) struct.put(FieldName.SOURCE, source);
        if (timestamp != null) struct.put(FieldName.TIMESTAMP, timestamp.toEpochMilli());
        return struct;
    }

    /*
     * @Description:读事件
     * @Param: record
     * @param: source
     * @param: timestamp
     * @return: com.mn.cdc.structure.Struct
     * @Author: miaoneng
     * @Date: 2021/12/1 10:09
     */
    public Struct read(Object record, Struct source, Instant timestamp) {
        Struct struct = new Struct(schema);
        struct.put(FieldName.OPERATION, Operation.READ.code());
        struct.put(FieldName.AFTER, record);
        if (source != null) struct.put(FieldName.SOURCE, source);
        if (timestamp != null) struct.put(FieldName.TIMESTAMP, timestamp.toEpochMilli());
        return struct;
    }


    private Envelope(Schema schema) {
        this.schema = schema;
    }


}
