package org.zp.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.zp.conf.DatabaseConfig;
import org.zp.entity.DataBaseInfo;
import org.zp.entity.ProcessInfo;
import org.zp.entity.SqlFile;
import org.zp.service.ExcuteSqlService;
import org.zp.service.GenerateSqlService;
import org.zp.service.TransferringDataService;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author zp
 * @Date 2024/10/10 14:19
 */


@RestController
@RequestMapping("/api/request")
public class MysqlToGaussDBSyncController {

    private final GenerateSqlService generateSqlService;

    private final ExcuteSqlService excuteSqlService;

    private final TransferringDataService transferringDataService;

    private final DatabaseConfig databaseConfig;


    public MysqlToGaussDBSyncController(GenerateSqlService generateSqlService, ExcuteSqlService excuteSqlService, TransferringDataService transferringDataService, DatabaseConfig databaseConfig) {
        this.generateSqlService = generateSqlService;
        this.excuteSqlService = excuteSqlService;
        this.transferringDataService = transferringDataService;
        this.databaseConfig = databaseConfig;
    }

    @GetMapping("/testConnection")
    @ResponseBody
    public ResponseEntity<Object> testConnection() {
        Map<Object, Object> resultMap = new HashMap<>();
        DataBaseInfo mysql = databaseConfig.getMysqlDataBaseInfo();
        DataBaseInfo gauss = databaseConfig.getGaussDataBaseInfo();
        if (excuteSqlService.testConnection(mysql)) {
            if (excuteSqlService.testConnection(gauss)) {
                resultMap.put("msg", "connection successful");
                return new ResponseEntity<>(resultMap, HttpStatus.OK);
            } else {
                resultMap.put("msg", "gauss connection failed");
                return new ResponseEntity<>(resultMap, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } else {
            resultMap.put("msg", "mysql connection failed");
            return new ResponseEntity<>(resultMap, HttpStatus.INTERNAL_SERVER_ERROR);

        }
    }


    @GetMapping("/startSync/{taskId}")
    @ResponseBody
    public ResponseEntity<Object> startSync(@PathVariable(name = "taskId") String taskId) {
        Map<Object, Object> resultMap = new HashMap<>();
        DataBaseInfo mysql = databaseConfig.getMysqlDataBaseInfo();
        DataBaseInfo gauss = databaseConfig.getGaussDataBaseInfo();
        if (generateSqlService.generateSql(mysql, taskId) &&
                excuteSqlService.executeSqlFile(gauss, mysql.getSchema() + SqlFile.STRUCTURE.getFileName(), taskId)) {
            transferringDataService.startSync(mysql, gauss, taskId);
            //执行其他sql
            ProcessInfo processInfo = ProcessInfo.getTaskProgressMap().get(taskId);
            excuteSqlService.executeSqlFile(gauss, mysql.getSchema() + SqlFile.INDEXES.getFileName(), taskId + 1);
            processInfo.setProcess(92.00);
            excuteSqlService.executeSqlFile(gauss, mysql.getSchema() + SqlFile.FOREIGN_KEYS.getFileName(), taskId + 1);
            processInfo.setProcess(95.00);
            excuteSqlService.executeSqlFile(gauss, mysql.getSchema() + SqlFile.CHECK_CONSTRAINTS.getFileName(), taskId + 1);
            processInfo.setProcess(96.00);
            excuteSqlService.executeSqlFile(gauss, mysql.getSchema() + SqlFile.VIEWS.getFileName(), taskId + 1);
            processInfo.setProcess(100.00);
            processInfo.setTaskStatus(ProcessInfo.TaskStatus.SUCCESS);
            resultMap.put("msg", "ok");
            return new ResponseEntity<>(resultMap, HttpStatus.OK);
        }
        resultMap.put("msg", "error");
        return new ResponseEntity<>(resultMap, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @GetMapping("/getProgress/{taskId}")
    @ResponseBody
    public ResponseEntity<Object> getProgress(@PathVariable(name = "taskId") String taskId) {
        ProcessInfo processInfo = ProcessInfo.getTaskProgressMap().get(taskId);
        if (processInfo == null) {
            processInfo = new ProcessInfo(0.00, ProcessInfo.TaskStatus.INIT, ProcessInfo.TaskType.GENERATE_SQL);
        }
        return new ResponseEntity<>(processInfo, HttpStatus.OK);
    }


}
