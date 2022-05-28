/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mn.cdc.oracle.entity;

import com.mn.cdc.relational.Column;
import com.mn.cdc.structure.BaseRecord;

import java.math.BigInteger;
import java.util.List;

/**
 * Date: 2021/08/13 Company: www.dtstack.com
 *
 * @author dujie
 *     <p>v$logmnr_contents 对应的实体 logminer读取出的数据实体
 */
public class RecordLog implements BaseRecord {

    private BigInteger scn;
    /** redo语句* */
    private String sqlRedo;
    /** 事务id撤销段号**/
    private String xidUsn;
    /** 事务id槽号**/
    private String xidSlt;
    /** 事务id序列号**/
    private String xidSqn;
    /** rowId**/
    private String rowId;

    /**表名**/
    private String tableName;
    /** DML操作类型 1插入 2删除 3 更新**/
    private int operationCode;

    public List<Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }

    private List<Column> columnList;
    public EventRow getEventRow() {
        return eventRow;
    }

    public void setEventRow(EventRow eventRow) {
        this.eventRow = eventRow;
    }

    //这边是解析redosql数据。将解析好的数据一块和他所在的行信息绑定了。一起poll出去
    private EventRow eventRow;

    public String getFeatureKey() {
        return featureKey;
    }

    public void setFeatureKey(String featureKey) {
        this.featureKey = featureKey;
    }

    /**特征字段**/
    private String featureKey;
    public RecordLog(
            BigInteger scn,
            String sqlRedo,
            String xidUsn,
            String xidSlt,
            String xidSqn,
            String rowId,
            int operationCode,
            String tableName,
            String featureKey,
            EventRow eventRow,
            List<Column> columnList) {
        this.scn = scn;
        this.sqlRedo = sqlRedo;
        this.xidUsn = xidUsn;
        this.xidSlt = xidSlt;
        this.xidSqn = xidSqn;
        this.rowId = rowId;
        this.operationCode = operationCode;
        this.tableName = tableName;
        this.featureKey = featureKey;
        this.eventRow = eventRow;
        this.columnList = columnList;
    }

    public BigInteger getScn() {
        return scn;
    }

    public void setScn(BigInteger scn) {
        this.scn = scn;
    }


    public String getSqlRedo() {
        return sqlRedo;
    }

    public void setSqlRedo(String sqlRedo) {
        this.sqlRedo = sqlRedo;
    }

    public int getOperationCode() {
        return operationCode;
    }

    public void setOperationCode(int operationCode) {
        this.operationCode = operationCode;
    }

    public String getXidUsn() {
        return xidUsn;
    }

    public void setXidUsn(String xidUsn) {
        this.xidUsn = xidUsn;
    }

    public String getXidSlt() {
        return xidSlt;
    }

    public void setXidSlt(String xidSlt) {
        this.xidSlt = xidSlt;
    }

    public String getXidSqn() {
        return xidSqn;
    }

    public void setXidSqn(String xidSqn) {
        this.xidSqn = xidSqn;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "RecordLog{" +
                "scn=" + scn +
                ", sqlRedo='" + sqlRedo + '\'' +
                ", xidUsn='" + xidUsn + '\'' +
                ", xidSlt='" + xidSlt + '\'' +
                ", xidSqn='" + xidSqn + '\'' +
                ", rowId='" + rowId + '\'' +
                ", tableName='" + tableName + '\'' +
                ", operationCode=" + operationCode +
                ", featureKey='" + featureKey + '\'' +
                '}';
    }
}
