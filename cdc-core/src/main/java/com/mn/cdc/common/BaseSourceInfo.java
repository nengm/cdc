/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.mn.cdc.common;


import com.mn.cdc.config.CommonEngineConfig;
import com.mn.cdc.basesourceinfo.AbstractSourceInfo;

public abstract class BaseSourceInfo extends AbstractSourceInfo {

    public BaseSourceInfo(CommonEngineConfig config) {
        super(config);
    }

}
