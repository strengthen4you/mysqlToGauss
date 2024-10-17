package org.zp.service;

import org.zp.entity.DataBaseInfo;

/**
 * @Author zp
 * @Date 2024/10/10 14:17
 */

public interface ExcuteSqlService {

    boolean executeSqlFile(DataBaseInfo dataBaseInfo, String fileName, String taskId);

    boolean testConnection(DataBaseInfo dataBaseInfo);
}
