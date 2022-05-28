package com.mn.cdc.relational;

import com.mn.cdc.structure.Struct;

/**
 * @program:cdc-master
 * @description
 * @author:miaoneng
 * @create:2021-11-29 17:55
 **/
@FunctionalInterface
public interface StructGenerator {
    /**
     * Converts the given tuple into a corresponding change event key or value struct.
     */
    Struct generateValue(Object[] values);
}
