package org.zp.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author zp
 * @Date 2024/10/16 18:33
 */

public class DefaultFunctionMappings {
    public static final Map<String, String> MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS = new HashMap<>();

    static {
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("CURRENT_TIMESTAMP(6)", "CURRENT_TIMESTAMP(6)");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("CURRENT_DATE", "CURRENT_DATE");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("CURRENT_TIME", "CURRENT_TIME");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("NOW()", "NOW()");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("LOCALTIME", "LOCALTIME");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("LOCALTIMESTAMP", "LOCALTIMESTAMP");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("UUID()", "gen_random_uuid()");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("RAND()", "random()");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("CURDATE()", "CURRENT_DATE");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("CURTIME()", "CURRENT_TIME");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("UNIX_TIMESTAMP()", "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("USER()", "CURRENT_USER");
        MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.put("DATABASE()", "CURRENT_DATABASE()");
    }

    public static String convertDefaultFunction(String mysqlFunction) {
        return MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS.getOrDefault(mysqlFunction.toUpperCase(), mysqlFunction);
    }

    public static Map<String, String> getMysqlToGaussDefaultFunctions() {
        return MYSQL_TO_GAUSS_DEFAULT_FUNCTIONS;
    }
}
