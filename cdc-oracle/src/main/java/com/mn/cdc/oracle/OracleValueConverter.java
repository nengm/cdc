package com.mn.cdc.oracle;

import com.mn.cdc.data.Bits;
import com.mn.cdc.data.SpecialValueDecimal;
import com.mn.cdc.data.Xml;
import com.mn.cdc.relational.Column;
import com.mn.cdc.structure.SchemaBuilder;
import com.mn.cdc.time.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;

/**
 * @program:cdc
 * @description
 * @author:miaoneng
 * @create:2022-04-26 14:28
 **/
public class OracleValueConverter {
    Logger logger = LoggerFactory.getLogger(OracleValueConverter.class);
    public SchemaBuilder schemaBuilder(Column column){
        switch (column.jdbcType()){
            case Types.NULL:
                logger.warn("Unexpected JDBC type: NULL");
                return null;

            // Single- and multi-bit values ...
            case Types.BIT:
                if (column.length() > 1) {
                    return Bits.builder(column.length());
                }
                // otherwise, it is just one bit so use a boolean ...
            case Types.BOOLEAN:
                return SchemaBuilder.bool();

            // Fixed-length binary values ...
            // 都用string来转
            case Types.BLOB:
            case Types.BINARY:
                return SchemaBuilder.string();

            // Variable-length binary values ...
            // 都用string来转
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return SchemaBuilder.string();

            // Numeric integers
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255
                return SchemaBuilder.int8();
            case Types.SMALLINT:
                // values are a 16-bit signed integer value between -32768 and 32767
                return SchemaBuilder.int16();
            case Types.INTEGER:
                // values are a 32-bit signed integer value between - 2147483648 and 2147483647
                return SchemaBuilder.int32();
            case Types.BIGINT:
                // values are a 64-bit signed integer value between -9223372036854775808 and 9223372036854775807
                return SchemaBuilder.int64();

            // Numeric decimal numbers
            case Types.REAL:
                // values are single precision floating point number which supports 7 digits of mantissa.
                return SchemaBuilder.float32();
            case Types.FLOAT:
            case Types.DOUBLE:
                //精读改变，方便处理，有额外需求再修改
            case Types.NUMERIC:
            case Types.DECIMAL:
                // values are double precision floating point number which supports 15 digits of mantissa.
                return SchemaBuilder.float64();

            // Fixed-length string values
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.NCLOB:

                // Variable-length string values
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.DATALINK:
                return SchemaBuilder.string();
            case Types.SQLXML:
                return Xml.builder();
            // Date and time values
            case Types.DATE:
                return Date.builder();
            case Types.TIME:
                return Time.builder();
            case Types.TIMESTAMP:
                return Timestamp.builder();
            case Types.TIME_WITH_TIMEZONE:
                return ZonedTime.builder();
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return ZonedTimestamp.builder();

            // Other types ...
            case Types.ROWID:
                // often treated as a string, but we'll generalize and treat it as a byte array
                return SchemaBuilder.bytes();

            // Unhandled types
            case Types.DISTINCT:
            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.STRUCT:
            default:
                break;
        }
        return null;
    }
}
