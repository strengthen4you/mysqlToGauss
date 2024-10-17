package org.zp.entity;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author zp
 * @Date 2024/10/16 13:58
 */

public class DataTypeMappings {

    private static final Map<String, String> DATA_TYPE_MAPPINGS;

    static {
        Map<String, String> tempMap = new HashMap<>();
        tempMap.put("BLOB", "BYTEA");
        tempMap.put("LONGBLOB", "BYTEA");
        tempMap.put("MEDIUMBLOB", "BYTEA");
        tempMap.put("TINYBLOB", "BYTEA");
        tempMap.put("DATETIME", "TIMESTAMP");
        tempMap.put("LONGTEXT", "TEXT");
        tempMap.put("DOUBLE", "FLOAT8");
        tempMap.put("MEDIUMTEXT", "TEXT");
        tempMap.put("TINYTEXT", "VARCHAR(255)");
        tempMap.put("VARCHAR", "VARCHAR(%d)");
        tempMap.put("CHAR", "CHAR(%d)");
        tempMap.put("BIT", "BOOLEAN");
        tempMap.put("TINYINT", "SMALLINT");
        tempMap.put("FLOAT", "REAL");
        tempMap.put("DECIMAL", "NUMERIC");
        tempMap.put("YEAR", "INTEGER");
        tempMap.put("BINARY", "BYTEA");
        tempMap.put("VARBINARY", "BYTEA");
        tempMap.put("SET", "ARRAY");
        tempMap.put("JSON", "JSONB");

        //tempMap.put("TIMESTAMP", "TIMESTAMP WITH TIME ZONE");
        //默认不带时区
        tempMap.put("TIMESTAMP", "TIMESTAMP");

        tempMap.put("ENUM", "ENUM");
        tempMap.put("BOOLEAN", "BOOLEAN");
        tempMap.put("SERIAL", "SERIAL");
        tempMap.put("DATE", "DATE");
        tempMap.put("TIME", "TIME");
        tempMap.put("SMALLINT", "SMALLINT");
        tempMap.put("MEDIUMINT", "INTEGER");
        tempMap.put("INT", "INTEGER");
        tempMap.put("BIGINT", "BIGINT");
        tempMap.put("GEOMETRY", "GEOMETRY");
        tempMap.put("POINT", "POINT");
        tempMap.put("LINESTRING", "LINESTRING");
        tempMap.put("POLYGON", "POLYGON");
        DATA_TYPE_MAPPINGS = Collections.unmodifiableMap(tempMap);
    }

    public static Map<String, String> getDataTypeMappings() {
        return DATA_TYPE_MAPPINGS;
    }


}
