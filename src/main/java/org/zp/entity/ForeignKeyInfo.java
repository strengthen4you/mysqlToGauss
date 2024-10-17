package org.zp.entity;

/**
 * @Author zp
 * @Date 2024/10/12 18:10
 */

public class ForeignKeyInfo {

    private final String fkColumnName;
    private final String pkTableName;
    private final String pkColumnName;
    private final short updateRule;
    private final short deleteRule;

    public ForeignKeyInfo(String fkColumnName, String pkTableName, String pkColumnName, short updateRule, short deleteRule) {
        this.fkColumnName = fkColumnName;
        this.pkTableName = pkTableName;
        this.pkColumnName = pkColumnName;
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
    }

    public String getFkColumnName() {
        return fkColumnName;
    }

    public String getPkTableName() {
        return pkTableName;
    }

    public String getPkColumnName() {
        return pkColumnName;
    }

    public short getUpdateRule() {
        return updateRule;
    }

    public short getDeleteRule() {
        return deleteRule;
    }
}
