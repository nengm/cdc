package com.mn.cdc.structure;

import com.mn.cdc.exception.DataException;
import com.mn.cdc.exception.SchemaBuilderException;

import java.util.*;

/**
 * @program:cdc-master
 * @description Schema生成器
 * @author:miaoneng
 * @create:2021-09-09 15:22
 **/
public class SchemaBuilder implements Schema{
    private static final String TYPE_FIELD = "type";
    private static final String OPTIONAL_FIELD = "optional";
    private static final String DEFAULT_FIELD = "default";
    private static final String NAME_FIELD = "name";
    private static final String VERSION_FIELD = "version";
    private static final String DOC_FIELD = "doc";


    private final Type type;
    private Boolean optional = null;
    private Object defaultValue = null;

    private Map<String, Field> fields = null;
    private Schema keySchema = null;
    private Schema valueSchema = null;

    private String name;
    private Integer version;
    // Optional human readable documentation describing this schema.
    private String doc;
    // Additional parameters for logical types.
    private Map<String, String> parameters;

    public SchemaBuilder(Type type) {
        if (null == type)
            throw new SchemaBuilderException("type cannot be null");
        this.type = type;
        if (type == Type.STRUCT) {
            fields = new LinkedHashMap<>();
        }
    }

    // Common/metadata fields

    @Override
    public boolean isOptional() {
        return optional == null ? false : optional;
    }

    /**
     * Set this schema as optional.
     * @return the SchemaBuilder
     */
    public SchemaBuilder optional() {
        checkCanSet(OPTIONAL_FIELD, optional, true);
        optional = true;
        return this;
    }

    /**
     * Set this schema as required. This is the default, but this method can be used to make this choice explicit.
     * @return the SchemaBuilder
     */
    public SchemaBuilder required() {
        checkCanSet(OPTIONAL_FIELD, optional, false);
        optional = false;
        return this;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    /**
     * Set the default value for this schema. The value is validated against the schema type, throwing a
     * {@link SchemaBuilderException} if it does not match.
     * @param value the default value
     * @return the SchemaBuilder
     */
    public SchemaBuilder defaultValue(Object value) {
        checkCanSet(DEFAULT_FIELD, defaultValue, value);
        checkNotNull(TYPE_FIELD, type, DEFAULT_FIELD);
        try {
            ConnectSchema.validateValue(this, value);
        } catch (DataException e) {
            throw new SchemaBuilderException("Invalid default value", e);
        }
        defaultValue = value;
        return this;
    }

    @Override
    public String name() {
        return name;
    }

    /**
     * Set the name of this schema.
     * @param name the schema name
     * @return the SchemaBuilder
     */
    public SchemaBuilder name(String name) {
        checkCanSet(NAME_FIELD, this.name, name);
        this.name = name;
        return this;
    }

    @Override
    public Integer version() {
        return version;
    }

    /**
     * Set the version of this schema. Schema versions are integers which, if provided, must indicate which schema is
     * newer and which is older by their ordering.
     * @param version the schema version
     * @return the SchemaBuilder
     */
    public SchemaBuilder version(Integer version) {
        checkCanSet(VERSION_FIELD, this.version, version);
        this.version = version;
        return this;
    }

    @Override
    public String doc() {
        return doc;
    }

    /**
     * Set the documentation for this schema.
     * @param doc the documentation
     * @return the SchemaBuilder
     */
    public SchemaBuilder doc(String doc) {
        checkCanSet(DOC_FIELD, this.doc, doc);
        this.doc = doc;
        return this;
    }

    @Override
    public Map<String, String> parameters() {
        return parameters == null ? null : Collections.unmodifiableMap(parameters);
    }

    /**
     * Set a schema parameter.
     * @param propertyName name of the schema property to define
     * @param propertyValue value of the schema property to define, as a String
     * @return the SchemaBuilder
     */
    public SchemaBuilder parameter(String propertyName, String propertyValue) {
        // Preserve order of insertion with a LinkedHashMap. This isn't strictly necessary, but is nice if logical types
        // can print their properties in a consistent order.
        if (parameters == null)
            parameters = new LinkedHashMap<>();
        parameters.put(propertyName, propertyValue);
        return this;
    }

    /**
     * Set schema parameters. This operation is additive; it does not remove existing parameters that do not appear in
     * the set of properties pass to this method.
     * @param props Map of properties to set
     * @return the SchemaBuilder
     */
    public SchemaBuilder parameters(Map<String, String> props) {
        // Avoid creating an empty set of properties so we never have an empty map
        if (props.isEmpty())
            return this;
        if (parameters == null)
            parameters = new LinkedHashMap<>();
        parameters.putAll(props);
        return this;
    }

    @Override
    public Type type() {
        return type;
    }

    /**
     * Create a SchemaBuilder for the specified type.
     *
     * Usually it will be simpler to use one of the variants like {@link #string()} or {@link #struct()}, but this form
     * can be useful when generating schemas dynamically.
     *
     * @param type the schema type
     * @return a new SchemaBuilder
     */
    public static SchemaBuilder type(Type type) {
        return new SchemaBuilder(type);
    }

    // Primitive types

    /**
     * @return a new {@link Type#INT8} SchemaBuilder
     */
    public static SchemaBuilder int8() {
        return new SchemaBuilder(Type.INT8);
    }

    /**
     * @return a new {@link Type#INT16} SchemaBuilder
     */
    public static SchemaBuilder int16() {
        return new SchemaBuilder(Type.INT16);
    }

    /**
     * @return a new {@link Type#INT32} SchemaBuilder
     */
    public static SchemaBuilder int32() {
        return new SchemaBuilder(Type.INT32);
    }

    /**
     * @return a new {@link Type#INT64} SchemaBuilder
     */
    public static SchemaBuilder int64() {
        return new SchemaBuilder(Type.INT64);
    }

    /**
     * @return a new {@link Type#FLOAT32} SchemaBuilder
     */
    public static SchemaBuilder float32() {
        return new SchemaBuilder(Type.FLOAT32);
    }

    /**
     * @return a new {@link Type#FLOAT64} SchemaBuilder
     */
    public static SchemaBuilder float64() {
        return new SchemaBuilder(Type.FLOAT64);
    }

    /**
     * @return a new {@link Type#BOOLEAN} SchemaBuilder
     */
    public static SchemaBuilder bool() {
        return new SchemaBuilder(Type.BOOLEAN);
    }

    /**
     * @return a new {@link Type#STRING} SchemaBuilder
     */
    public static SchemaBuilder string() {
        return new SchemaBuilder(Type.STRING);
    }

    /**
     * @return a new {@link Type#BYTES} SchemaBuilder
     */
    public static SchemaBuilder bytes() {
        return new SchemaBuilder(Type.BYTES);
    }


    // Structs

    /**
     * @return a new {@link Type#STRUCT} SchemaBuilder
     */
    public static SchemaBuilder struct() {
        return new SchemaBuilder(Type.STRUCT);
    }

    /**
     * Add a field to this struct schema. Throws a SchemaBuilderException if this is not a struct schema.
     * @param fieldName the name of the field to add
     * @param fieldSchema the Schema for the field's value
     * @return the SchemaBuilder
     */
    public SchemaBuilder field(String fieldName, Schema fieldSchema) {
        if (type != Type.STRUCT)
            throw new SchemaBuilderException("Cannot create fields on type " + type);
        if (null == fieldName || fieldName.isEmpty())
            throw new SchemaBuilderException("fieldName cannot be null.");
        if (null == fieldSchema)
            throw new SchemaBuilderException("fieldSchema for field " + fieldName + " cannot be null.");
        int fieldIndex = fields.size();
        if (fields.containsKey(fieldName))
            throw new SchemaBuilderException("Cannot create field because of field name duplication " + fieldName);
        fields.put(fieldName, new Field(fieldName, fieldIndex, fieldSchema));
        return this;
    }

    /**
     * Get the list of fields for this Schema. Throws a DataException if this schema is not a struct.
     * @return the list of fields for this Schema
     */
    @Override
    public List<Field> fields() {
        if (type != Type.STRUCT)
            throw new DataException("Cannot list fields on non-struct type");
        return new ArrayList<>(fields.values());
    }

    @Override
    public Field field(String fieldName) {
        if (type != Type.STRUCT)
            throw new DataException("Cannot look up fields on non-struct type");
        return fields.get(fieldName);
    }



    // Maps & Arrays

    /**
     * @param valueSchema the schema for elements of the array
     * @return a new {@link Type#ARRAY} SchemaBuilder
     */
    public static SchemaBuilder array(Schema valueSchema) {
        if (null == valueSchema)
            throw new SchemaBuilderException("valueSchema cannot be null.");
        SchemaBuilder builder = new SchemaBuilder(Type.ARRAY);
        builder.valueSchema = valueSchema;
        return builder;
    }

    /**
     * @param keySchema the schema for keys in the map
     * @param valueSchema the schema for values in the map
     * @return a new {@link Type#MAP} SchemaBuilder
     */
    public static SchemaBuilder map(Schema keySchema, Schema valueSchema) {
        if (null == keySchema)
            throw new SchemaBuilderException("keySchema cannot be null.");
        if (null == valueSchema)
            throw new SchemaBuilderException("valueSchema cannot be null.");
        SchemaBuilder builder = new SchemaBuilder(Type.MAP);
        builder.keySchema = keySchema;
        builder.valueSchema = valueSchema;
        return builder;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    @Override
    public Schema valueSchema() {
        return valueSchema;
    }


    /**
     * Build the Schema using the current settings
     * @return the {@link Schema}
     */
    public Schema build() {
        return new ConnectSchema(type, isOptional(), defaultValue, name, version, doc,
                parameters == null ? null : Collections.unmodifiableMap(parameters),
                fields == null ? null : Collections.unmodifiableList(new ArrayList<Field>(fields.values())), keySchema, valueSchema);
    }

    /**
     * Return a concrete instance of the {@link Schema} specified by this builder
     * @return the {@link Schema}
     */
    @Override
    public Schema schema() {
        return build();
    }

    private static void checkCanSet(String fieldName, Object fieldVal, Object val) {
        if (fieldVal != null && fieldVal != val)
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " has already been set.");
    }

    private static void checkNotNull(String fieldName, Object val, String fieldToSet) {
        if (val == null)
            throw new SchemaBuilderException("Invalid SchemaBuilder call: " + fieldName + " must be specified to set " + fieldToSet);
    }
}
