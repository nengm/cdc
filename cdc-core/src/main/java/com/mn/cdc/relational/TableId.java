package com.mn.cdc.relational;

import com.mn.cdc.schema.DataCollectionId;

/**
 * @program:cdc-master
 * @description 表的唯一标识
 * @author:miaoneng
 * @create:2021-09-09 13:46
 **/
public class TableId implements DataCollectionId,Comparable<TableId> {

    //一个数据库系统包含多个Catalog，每个Catalog又包含多个Schema，而每个Schema又包含多个数 据库对象（表、视图、字段等），反过来讲一个数据库对象必然属于一个Schema，而该Schema又必然属于一个Catalog

    //catalog名称，包含表的数据库目录的名称，如果数据库驱动没有展示，就有可能为null
    private final String catalogName;

    //shcema名称，包含表的数据库模式的名称，如果数据库驱动没有展示，就有可能为null
    private final String schemaName;

    //表名，不为空
    private final String tableName;

    private final String id;

    public TableId(String catalogName, String schemaName, String tableName) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        assert this.tableName != null;
        this.id = tableId(this.catalogName, this.schemaName, this.tableName);
    }
    /**
     * Create a new table identifier.
     *
     * @param catalogName the name of the database catalog that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param schemaName the name of the database schema that contains the table; may be null if the JDBC driver does not
     *            show a schema for this table
     * @param tableName the name of the table; may not be null
     * @param tableIdMapper the customization of fully quailified table name
     */
    public TableId(String catalogName, String schemaName, String tableName, Selectors.TableIdToStringMapper tableIdMapper) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        assert this.tableName != null;
        this.id = tableIdMapper == null ? tableId(this.catalogName, this.schemaName, this.tableName) : tableIdMapper.toString(this);
    }
    /*
     * @Description:组装表的标识符
     * @Param: catalog
     * @param: schema
     * @param: table
     * @return: java.lang.String
     * @Author: miaoneng
     * @Date: 2021/9/11 19:35
     */
    private static String tableId(String catalog, String schema, String table) {
        if (catalog == null || catalog.length() == 0) {
            if (schema == null || schema.length() == 0) {
                return table;
            }
            return schema + "." + table;
        }
        if (schema == null || schema.length() == 0) {
            return catalog + "." + table;
        }
        return catalog + "." + schema + "." + table;
    }


    @Override
    public int compareTo(TableId that) {
        if (this == that) return 0;
        return this.id.compareTo(that.id);
    }

    public TableId toLowercase() {
        return new TableId(catalogName, schemaName, tableName.toLowerCase());
    }
    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableId) {
            return this.compareTo((TableId) obj) == 0;
        }
        return false;
    }
    /*
     * @Description:jdbc所在目录名称，例如mysql是数据库名
     * @Param:
     * @return: java.lang.String
     * @Author: miaoneng
     * @Date: 2021/9/11 11:04
     */
    public String catalog() {
        return catalogName;
    }

    /*
     * @Description:jdbc模式，例如mysql是null
     * @Param:
     * @return: java.lang.String
     * @Author: miaoneng
     * @Date: 2021/9/11 11:06
     */
    public String schema() {
        return schemaName;
    }

    /*
     * @Description:得到表名。永远不能为null
     * @Param:
     * @return: java.lang.String
     * @Author: miaoneng
     * @Date: 2021/9/11 11:12
     */
    public String table() {
        return tableName;
    }
    public String id() {
        return id;
    }
    public static TableId parse(String str) {
        return parse(str, true);
    }
    public static TableId parse(String str, boolean useCatalogBeforeSchema) {
        String[] parts = TableIdParser.parse(str).stream()
                .toArray(String[]::new);

        return TableId.parse(parts, parts.length, useCatalogBeforeSchema);
    }
    /*
     * @Description:解析提供的字符串，将前3部分提取到TableID中。
     * @Param: parts 标识的各部分，不能为空
     * @param: numParts 用于表标识parts所用到的个数
     * @param: useCatalogBeforeSchema 使用catalog还是使用schema，true：使用catalog+表名，false：使用shcema+表名。例如：mysql：catalog是miaon，schema是null，表名是baseinfo，则TableId（miaon，null，baseinfo）
     * @return: com.mn.cdc.relational.TableId
     * @Author: miaoneng
     * @Date: 2021/9/11 11:16
     */
    protected static TableId parse(String[] parts, int numParts, boolean useCatalogBeforeSchema) {
        if (numParts == 0) return null;
        if (numParts == 1) return new TableId(null, null, parts[0]); // table only
        if (numParts == 2) {
            if (useCatalogBeforeSchema) return new TableId(parts[0], null, parts[1]); // catalog & table only
            return new TableId(null, parts[0], parts[1]); // schema & table only
        }
        return new TableId(parts[0], parts[1], parts[2]); // catalog, schema & table
    }

    /**
     * Returns a dot-separated String representation of this identifier, quoting all
     * name parts with the {@code "} char.
     */
    public String toDoubleQuotedString() {
        return toQuotedString('"');
    }
    /**
     * 根据给定的前缀，返回点分隔的字符串，同时用给定字符串包裹所有的前缀
     */
    public String toQuotedString(char quotingChar) {
        StringBuilder quoted = new StringBuilder();

        if (catalogName != null && !catalogName.isEmpty()) {
            quoted.append(quote(catalogName, quotingChar)).append(".");
        }

        if (schemaName != null && !schemaName.isEmpty()) {
            quoted.append(quote(schemaName, quotingChar)).append(".");
        }

        quoted.append(quote(tableName, quotingChar));

        return quoted.toString();
    }

    /**
     * Quotes the given identifier part, e.g. schema or table name.
     */
    private static String quote(String identifierPart, char quotingChar) {
        if (identifierPart == null) {
            return null;
        }

        if (identifierPart.isEmpty()) {
            return new StringBuilder().append(quotingChar).append(quotingChar).toString();
        }

        if (identifierPart.charAt(0) != quotingChar && identifierPart.charAt(identifierPart.length() - 1) != quotingChar) {
            identifierPart = identifierPart.replace(quotingChar + "", repeat(quotingChar));
            identifierPart = quotingChar + identifierPart + quotingChar;
        }

        return identifierPart;
    }

    private static String repeat(char quotingChar) {
        return new StringBuilder().append(quotingChar).append(quotingChar).toString();
    }

    /*
     * @Description:转双引号
     * @Param:
     * @return: com.mn.cdc.relational.TableId
     * @Author: miaoneng
     * @Date: 2021/12/1 20:25
     */
    public TableId toDoubleQuoted() {
        return toQuoted('"');
    }

    public TableId toQuoted(char quotingChar) {
        String catalogName = null;
        if (this.catalogName != null && !this.catalogName.isEmpty()) {
            catalogName = quote(this.catalogName, quotingChar);
        }

        String schemaName = null;
        if (this.schemaName != null && !this.schemaName.isEmpty()) {
            schemaName = quote(this.schemaName, quotingChar);
        }

        return new TableId(catalogName, schemaName, quote(this.tableName, quotingChar));
    }
}
