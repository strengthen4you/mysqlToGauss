package org.zp.entity;

/**
 * @Author zp
 * @Date 2024/10/12 18:09
 */

public class IndexInfo {

    private final String columnName;
    private final boolean unique;

    public IndexInfo(String columnName, boolean unique) {
        this.columnName = columnName;
        this.unique = unique;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isUnique() {
        return unique;
    }
}
