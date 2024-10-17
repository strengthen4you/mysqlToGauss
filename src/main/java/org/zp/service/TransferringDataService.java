package org.zp.service;

import org.zp.entity.DataBaseInfo;

/**
 * @Author zp
 * @Date 2024/10/14 17:11
 */

public interface TransferringDataService {

    void startSync(DataBaseInfo mysqlDataBaseInfo, DataBaseInfo gaussDataBaseInfo, String taskId);
}
