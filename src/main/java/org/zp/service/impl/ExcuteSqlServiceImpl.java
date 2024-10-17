package org.zp.service.impl;

import org.springframework.stereotype.Service;
import org.zp.entity.DataBaseInfo;
import org.zp.entity.ProcessInfo;
import org.zp.service.ExcuteSqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author zp
 * @Date 2024/10/10 14:18
 */

@Service
public class ExcuteSqlServiceImpl implements ExcuteSqlService {

    private static final Logger logger = LoggerFactory.getLogger(ExcuteSqlServiceImpl.class);

    @Override
    public boolean executeSqlFile(DataBaseInfo dataBaseInfo, String fileName, String taskId) {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(dataBaseInfo.getUrl(), dataBaseInfo.getUser(), dataBaseInfo.getPassword());
            conn.setAutoCommit(false);
            setSearchPath(conn, dataBaseInfo);
            List<String> sqlStatements = readSqlFile(fileName);
            boolean success = executeStatements(conn, sqlStatements, taskId);
            conn.commit();
            logger.info(fileName + "所有SQL语句执行成功！");
            return success;
        } catch (Exception e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException rollbackEx) {
                logger.error("回滚事务失败", rollbackEx);
                return false;
            }
            logger.error("执行SQL文件时发生错误", e);
            return false;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error("关闭数据库连接失败", e);
            }
        }
    }

    @Override
    public boolean testConnection(DataBaseInfo dataBaseInfo) {
        try {
            Connection conn = DriverManager.getConnection(dataBaseInfo.getUrl(), dataBaseInfo.getUser(), dataBaseInfo.getPassword());
            conn.prepareStatement("SELECT 1");
            conn.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void setSearchPath(Connection conn, DataBaseInfo dataBaseInfo) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET search_path TO " + dataBaseInfo.getSchema());
            logger.info("成功设置schema为: {}", dataBaseInfo.getSchema());
        }
    }

    private List<String> readSqlFile(String fileName) throws IOException {
        List<String> sqlStatements = new ArrayList<>();
        StringBuilder statement = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("--")) {
                    continue;
                }
                statement.append(line);
                if (line.endsWith(";")) {
                    sqlStatements.add(statement.toString());
                    statement.setLength(0);
                }
            }
        }

        logger.info("成功读取SQL文件，共有{}条SQL语句", sqlStatements.size());
        return sqlStatements;
    }

    private boolean executeStatements(Connection conn, List<String> sqlStatements, String taskId) {
        int totalStatements = sqlStatements.size();
        int successCount = 0;
        int failureCount = 0;
        ProcessInfo processInfo = new ProcessInfo(10.00, ProcessInfo.TaskStatus.RUNNING, ProcessInfo.TaskType.EXECUTE_SQL);
        for (int i = 0; i < sqlStatements.size(); i++) {
            String sql = sqlStatements.get(i);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                successCount++;
                logger.info("执行进度: [{}/{}] SQL执行成功", (i + 1), totalStatements);
                processInfo.setProcess(processInfo.getProcess()+ ((i + 1) / totalStatements) * 0.2);
                ProcessInfo.taskProgressMap.put(taskId, processInfo);
            } catch (SQLException e) {
                failureCount++;
                String errorMessage = String.format(
                        "[%s] 执行SQL失败: %s%n错误信息: %s%nSQL语句: %s%n%n",
                        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                        e.getMessage(),
                        e.getSQLState(),
                        sql
                );
                logger.error(errorMessage);
                processInfo.setTaskStatus(ProcessInfo.TaskStatus.FAIL);
                processInfo.setProcess(processInfo.getProcess() + ((i + 1) / totalStatements) * 0.2);
                ProcessInfo.taskProgressMap.put(taskId, processInfo);
            }
        }
        logger.info("SQL执行完成，总计: {}，成功: {}，失败: {}",
                totalStatements, successCount, failureCount);
        return failureCount == 0;
    }
}
