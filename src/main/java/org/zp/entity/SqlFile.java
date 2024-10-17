package org.zp.entity;

/**
 * @Author zp
 * @Date 2024/10/15 10:13
 */

public enum SqlFile {

    STRUCTURE("_table_structure.sql"),
    INDEXES("_indexes.sql"),
    FOREIGN_KEYS("_foreign_keys.sql"),
    CHECK_CONSTRAINTS("_check_constraints.sql"),
    VIEWS("_views.sql");


    private String fileName;

    SqlFile(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }


}
