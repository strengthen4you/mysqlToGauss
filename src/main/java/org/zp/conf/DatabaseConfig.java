package org.zp.conf;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.zp.entity.DataBaseInfo;

/**
 * @Author zp
 * @Date 2024/9/23 15:54
 */

@Configuration
public class DatabaseConfig {

    @Value("${mysql.url}")
    private String mysqlUrl;

    @Value("${mysql.username}")
    private String mysqlUser;

    @Value("${mysql.password}")
    private String mysqlPassword;

    @Value("${mysql.schema}")
    private String mysqlSchema;

    @Value("${gauss.url}")
    private String gaussUrl;

    @Value("${gauss.username}")
    private String gaussUser;

    @Value("${gauss.password}")
    private String gaussPassword;

    @Value("${gauss.schema}")
    private String gaussSchema;

    public DataBaseInfo getMysqlDataBaseInfo() {
        return new DataBaseInfo(mysqlUrl, mysqlUser, mysqlPassword, mysqlSchema);
    }

    public DataBaseInfo getGaussDataBaseInfo() {
        return new DataBaseInfo(gaussUrl, gaussUser, gaussPassword, gaussSchema);
    }
}
