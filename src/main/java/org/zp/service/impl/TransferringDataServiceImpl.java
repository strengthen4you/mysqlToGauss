package org.zp.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zp.entity.DataBaseInfo;
import org.zp.entity.DataTypeMappings;
import org.zp.entity.ProcessInfo;
import org.zp.service.TransferringDataService;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @Author zp
 * @Date 2024/10/14 15:03
 */
@Service
public class TransferringDataServiceImpl implements TransferringDataService {

    private static final Logger logger = LoggerFactory.getLogger(TransferringDataServiceImpl.class);

    private Set<String> errorTables = new HashSet<>();

    private static final Map<String, TypeConverter> TYPE_CONVERTERS = new HashMap<>();

    private SyncProgress progress = new SyncProgress();

    @Value("${batch.size:1000}")
    private Integer batchSize;


    private class SyncProgress {
        long totalRecords = 0;
        long processedRecords = 0;
        Map<String, Long> tableRecords = new HashMap<>();
        Map<String, Long> tableProcessed = new HashMap<>();
        int totalTables = 0;
        int processedTables = 0;

        void reset() {
            totalRecords = 0;
            processedRecords = 0;
            tableRecords.clear();
            tableProcessed.clear();
            totalTables = 0;
            processedTables = 0;
        }
    }

    static {
        initializeTypeConverters();
    }

    private long totalRecords = 0;
    private long processedRecords = 0;
    private LocalDateTime startTime;

    @Override
    public void startSync(DataBaseInfo mysqlDataBaseInfo, DataBaseInfo gaussDataBaseInfo, String taskId) {
        startTime = LocalDateTime.now();
        ProcessInfo processInfo = new ProcessInfo(30.00, ProcessInfo.TaskStatus.RUNNING, ProcessInfo.TaskType.TRANSFER_DATA);
        ProcessInfo.taskProgressMap.put(taskId, processInfo);
        logger.info("开始数据同步 - {}", startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        try (Connection mysqlConn = getConnection(mysqlDataBaseInfo);
             Connection gaussConn = getConnection(gaussDataBaseInfo)) {

            // 获取需要同步的表列表
            List<String> tables = getTables(mysqlConn.getMetaData());
            progress.totalTables = tables.size();
            // 首先统计所有表的记录数
            countAllTablesRecords(mysqlConn, tables);

            // 预检查所有表
            for (String table : tables) {
                try {
                    checkTableCompatibility(table.trim(), mysqlConn, gaussConn);
                } catch (SQLException e) {
                    logger.error("表 {} 预检查失败，将跳过该表", table.trim(), e);
                    errorTables.add(table.trim());
                }
            }
            // 同步表
            for (String table : tables) {
                if (!errorTables.contains(table.trim())) {
                    try {
                        syncTable(table.trim(), mysqlConn, gaussConn, taskId);
                        progress.processedTables++;
                    } catch (SQLException e) {
                        logger.error("同步表 {} 失败", table.trim(), e);
                        errorTables.add(table.trim());
                    }
                }
            }

            generateSyncReport();

            progress.reset();

        } catch (Exception e) {
            logger.error("同步过程发生错误", e);
        }
    }

    private boolean tableExists(String tableName, Connection conn) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getTables(null, null, tableName, null)) {
            return rs.next();
        }
    }

    private Map<String, String> getGaussTableColumns(Connection conn, String tableName)
            throws SQLException {
        Map<String, String> columns = new HashMap<>();
        try (ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME").toLowerCase();
                String dataType = rs.getString("TYPE_NAME").toUpperCase();
                columns.put(columnName, dataType);
            }
        }
        return columns;
    }

    private boolean isTypeCompatible(String mysqlType, String gaussType) {
        String expectedGaussType = DataTypeMappings.getDataTypeMappings().getOrDefault(mysqlType.toUpperCase(), mysqlType);
        return expectedGaussType.equalsIgnoreCase(gaussType);
    }

    private void countAllTablesRecords(Connection conn, List<String> tables) throws SQLException {
        for (String table : tables) {
            String trimmedTable = table.trim();
            if (!errorTables.contains(trimmedTable)) {
                long count = getTableRecordCount(conn, trimmedTable);
                progress.tableRecords.put(trimmedTable, count);
                progress.totalRecords += count;
                progress.tableProcessed.put(trimmedTable, 0L);
                logger.info("表 {} 总记录数: {}", trimmedTable, count);
            }
        }
        logger.info("所有表总记录数: {}", progress.totalRecords);
    }

    private long getTableRecordCount(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0;
        }
    }

    private void countTotalRecords(Connection conn, String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                totalRecords = rs.getLong(1);
                logger.info("表 {} 总记录数: {}", tableName, totalRecords);
            }
        }
    }

    private String generateSelectSql(String tableName, List<ColumnInfo> columns) {
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(columns.get(i).name);
        }
        sql.append(" FROM ").append(tableName);
        return sql.toString();
    }

    private String generateInsertSql(String tableName, List<ColumnInfo> columns) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        StringBuilder values = new StringBuilder(") VALUES (");

        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
                values.append(", ");
            }
            sql.append(columns.get(i).name);
            values.append("?");
        }

        return sql.toString() + values.toString() + ")";
    }

    private void executeBatch(PreparedStatement stmt, Connection conn) throws SQLException {
        try {
            int[] results = stmt.executeBatch();
            int successCount = 0;
            for (int result : results) {
                if (result >= 0) successCount++;
            }
            logger.debug("批处理执行完成: 成功 {}/{}", successCount, results.length);
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }

    private void displayProgress(String currentTable, String taskId) {
        // 计算当前表的进度
        long tableTotal = progress.tableRecords.get(currentTable);
        long tableProcessed = progress.tableProcessed.get(currentTable);
        double tableProgress = tableTotal == 0 ? 100 : (double) tableProcessed / tableTotal * 100;

        // 计算总体进度
        double totalProgress = progress.totalRecords == 0 ? 100 : (double) progress.processedRecords / progress.totalRecords * 100;

        // 计算预估剩余时间
        long elapsedSeconds = java.time.Duration.between(startTime, LocalDateTime.now()).getSeconds();
        double remainingSeconds = elapsedSeconds * ((100 - totalProgress) / totalProgress);
        logger.info("同步进度 - 表 {}: {}% ({}/{}), 总进度: {}% ({}/{}), 已处理表数: {}/{}, 预估剩余时间: {} 秒",
                currentTable,
                String.format("%.2f", tableProgress),
                tableProcessed,
                tableTotal,
                String.format("%.2f", totalProgress),
                progress.processedRecords,
                progress.totalRecords,
                progress.processedTables,
                progress.totalTables,
                remainingSeconds);
        ProcessInfo processInfo = ProcessInfo.getTaskProgressMap().get(taskId);
        processInfo.setProcess(30.00 + totalProgress * 0.6);
    }

    // TypeConverter接口定义
    private interface TypeConverter {
        Object convert(ResultSet rs, int columnIndex) throws SQLException;

        void setParameter(PreparedStatement stmt, int parameterIndex, Object value) throws SQLException;
    }

    private void generateSyncReport() {
        LocalDateTime endTime = LocalDateTime.now();
        long duration = java.time.Duration.between(startTime, endTime).getSeconds();

        StringBuilder report = new StringBuilder();
        report.append("\n=== 数据同步报告 ===\n");
        report.append("开始时间: ").append(startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).append("\n");
        report.append("结束时间: ").append(endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)).append("\n");
        report.append("总耗时: ").append(duration).append(" 秒\n");
        report.append("总记录数: ").append(progress.totalRecords).append("\n");
        report.append("已同步记录数: ").append(progress.processedRecords).append("\n");
        report.append("总表数: ").append(progress.totalTables).append("\n");
        report.append("成功同步表数: ").append(progress.processedTables).append("\n");
        report.append("失败表数: ").append(errorTables.size()).append("\n");
        report.append("同步失败的表: ").append(errorTables.isEmpty() ? "无" : String.join(", ", errorTables)).append("\n");

        // 添加每个表的同步详情
        report.append("\n各表同步详情:\n");
        progress.tableRecords.forEach((table, total) -> {
            long processed = progress.tableProcessed.get(table);
            double tableProgress = (double) processed / total * 100;
            report.append(String.format("表 %s: %d/%d (%.2f%%)\n",
                    table, processed, total, tableProgress));
        });

        report.append("==================\n");

        logger.info(report.toString());
    }

    private void checkTableCompatibility(String tableName, Connection mysqlConn, Connection gaussConn)
            throws SQLException {
        logger.info("检查表 {}", tableName);

        // 检查表是否存在
        if (!tableExists(tableName, mysqlConn)) {
            throw new SQLException("MySQL中不存在表: " + tableName);
        }
        if (!tableExists(tableName, gaussConn)) {
            throw new SQLException("GaussDB中不存在表: " + tableName);
        }
    }

    // 主要同步方法
    private void syncTable(String tableName, Connection mysqlConn, Connection gaussConn, String taskId)
            throws SQLException {
        logger.info("开始同步表: {}", tableName);

        // 获取表结构和类型映射信息
        List<ColumnInfo> columns = getTableColumns(mysqlConn, tableName);

        // 计算总记录数
        countTotalRecords(mysqlConn, tableName);

        // 准备SQL语句
        String selectSql = generateSelectSql(tableName, columns);
        String insertSql = generateInsertSql(tableName, columns);

        try (PreparedStatement mysqlStmt = mysqlConn.prepareStatement(selectSql);
             PreparedStatement gaussStmt = gaussConn.prepareStatement(insertSql);
             ResultSet rs = mysqlStmt.executeQuery()) {

            gaussConn.setAutoCommit(false);
            int batchCount = 0;
            long tableProcessed = 0;

            while (rs.next()) {
                for (int i = 0; i < columns.size(); i++) {
                    ColumnInfo column = columns.get(i);
                    Object value = column.converter.convert(rs, i + 1);
                    try {
                        column.converter.setParameter(gaussStmt, i + 1, value);
                    } catch (SQLException e) {
                        logger.error("转换数据时出错 - 表: {}, 列: {}, MySQL类型: {}, 高斯类型: {}, 值: {}",
                                tableName, column.name, column.mysqlType, column.gaussType, value);
                        throw e;
                    }
                }

                gaussStmt.addBatch();
                batchCount++;
                processedRecords++;
                tableProcessed++;
                progress.processedRecords++;
                progress.tableProcessed.put(tableName, tableProcessed);

                if (batchCount >= batchSize) {
                    executeBatch(gaussStmt, gaussConn);
                    batchCount = 0;
                    displayProgress(tableName, taskId);
                }
            }

            if (batchCount > 0) {
                executeBatch(gaussStmt, gaussConn);
                displayProgress(tableName, taskId);
            }

            gaussConn.commit();
            logger.info("表 {} 同步完成，共同步 {} 条记录", tableName, tableProcessed);

        } catch (SQLException e) {
            gaussConn.rollback();
            logger.error("同步表 {} 时发生错误", tableName, e);
            throw e;
        }
    }

    private Map<String, String> getTableStructure(Connection conn, String tableName)
            throws SQLException {
        Map<String, String> columnTypes = new HashMap<>();

        try (ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME").toLowerCase();
                String dataType = rs.getString("TYPE_NAME");
                columnTypes.put(columnName, dataType);
            }
        }

        return columnTypes;
    }

    private String generateInsertSql(String tableName, Map<String, String> columnTypes) {
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName + " (");
        StringBuilder values = new StringBuilder(") VALUES (");

        boolean first = true;
        for (String columnName : columnTypes.keySet()) {
            if (!first) {
                sql.append(", ");
                values.append(", ");
            }
            sql.append(columnName);
            values.append("?");
            first = false;
        }

        return sql.toString() + values.toString() + ")";
    }


    private Connection getConnection(DataBaseInfo dataBaseInfo) throws SQLException {
        return DriverManager.getConnection(
                dataBaseInfo.getUrl(),
                dataBaseInfo.getUser(),
                dataBaseInfo.getPassword()
        );
    }

    private static List<String> getTables(DatabaseMetaData metaData) throws SQLException {
        List<String> tables = new ArrayList<>();
        ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"});
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }

    private static void initializeTypeConverters() {
        // 数值类型转换
        TYPE_CONVERTERS.put("TINYINT", new TypeConverter() {
            @Override
            public Object convert(ResultSet rs, int columnIndex) throws SQLException {
                int value = rs.getInt(columnIndex);
                return rs.wasNull() ? null : value;
            }

            @Override
            public void setParameter(PreparedStatement stmt, int parameterIndex, Object value) throws SQLException {
                if (value == null) {
                    stmt.setNull(parameterIndex, Types.SMALLINT);
                } else if (value instanceof String) {
                    stmt.setInt(parameterIndex, Integer.parseInt((String) value));
                } else {
                    stmt.setInt(parameterIndex, ((Number) value).intValue());
                }
            }
        });

        // BLOB转bytea
        TYPE_CONVERTERS.put("BLOB", new TypeConverter() {
            @Override
            public Object convert(ResultSet rs, int columnIndex) throws SQLException {
                Blob blob = rs.getBlob(columnIndex);
                if (blob == null) return null;
                return blob.getBytes(1, (int) blob.length());
            }

            @Override
            public void setParameter(PreparedStatement stmt, int parameterIndex, Object value) throws SQLException {
                if (value == null) {
                    stmt.setNull(parameterIndex, Types.BINARY);
                } else {
                    stmt.setBytes(parameterIndex, (byte[]) value);
                }
            }
        });

        // VARCHAR等字符串类型
        TypeConverter stringConverter = new TypeConverter() {
            @Override
            public Object convert(ResultSet rs, int columnIndex) throws SQLException {
                return rs.getString(columnIndex);
            }

            @Override
            public void setParameter(PreparedStatement stmt, int parameterIndex, Object value) throws SQLException {
                if (value == null) {
                    stmt.setNull(parameterIndex, Types.VARCHAR);
                } else {
                    stmt.setString(parameterIndex, value.toString());
                }
            }
        };
        TYPE_CONVERTERS.put("VARCHAR", stringConverter);
        TYPE_CONVERTERS.put("CHAR", stringConverter);
        TYPE_CONVERTERS.put("TEXT", stringConverter);

        // 数值类型
        TypeConverter numberConverter = new TypeConverter() {
            @Override
            public Object convert(ResultSet rs, int columnIndex) throws SQLException {
                BigDecimal value = rs.getBigDecimal(columnIndex);
                return rs.wasNull() ? null : value;
            }

            @Override
            public void setParameter(PreparedStatement stmt, int parameterIndex, Object value) throws SQLException {
                if (value == null) {
                    stmt.setNull(parameterIndex, Types.NUMERIC);
                } else if (value instanceof String) {
                    stmt.setBigDecimal(parameterIndex, new BigDecimal((String) value));
                } else if (value instanceof BigDecimal) {
                    stmt.setBigDecimal(parameterIndex, (BigDecimal) value);
                } else {
                    stmt.setBigDecimal(parameterIndex, new BigDecimal(value.toString()));
                }
            }
        };
        TYPE_CONVERTERS.put("DECIMAL", numberConverter);
        TYPE_CONVERTERS.put("NUMERIC", numberConverter);

        // 日期时间类型
        TYPE_CONVERTERS.put("DATETIME", new TypeConverter() {
            @Override
            public Object convert(ResultSet rs, int columnIndex) throws SQLException {
                return rs.getTimestamp(columnIndex);
            }

            @Override
            public void setParameter(PreparedStatement stmt, int parameterIndex, Object value) throws SQLException {
                if (value == null) {
                    stmt.setNull(parameterIndex, Types.TIMESTAMP);
                } else if (value instanceof String) {
                    stmt.setTimestamp(parameterIndex, Timestamp.valueOf((String) value));
                } else {
                    stmt.setTimestamp(parameterIndex, (Timestamp) value);
                }
            }
        });
    }


    // 列信息类
    private static class ColumnInfo {
        String name;
        String mysqlType;
        String gaussType;
        TypeConverter converter;

        public ColumnInfo(String name, String mysqlType, String gaussType, TypeConverter converter) {
            this.name = name;
            this.mysqlType = mysqlType;
            this.gaussType = gaussType;
            this.converter = converter;
        }
    }

    private List<ColumnInfo> getTableColumns(Connection conn, String tableName) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();

        try (ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String mysqlType = rs.getString("TYPE_NAME").toUpperCase();

                // 获取对应的高斯类型
                String gaussType = DataTypeMappings.getDataTypeMappings().getOrDefault(mysqlType.toUpperCase(), mysqlType);

                // 获取类型转换器
                TypeConverter converter = TYPE_CONVERTERS.getOrDefault(mysqlType,
                        new TypeConverter() {
                            @Override
                            public Object convert(ResultSet rs, int columnIndex) throws SQLException {
                                return rs.getObject(columnIndex);
                            }

                            @Override
                            public void setParameter(PreparedStatement stmt, int parameterIndex, Object value)
                                    throws SQLException {
                                stmt.setObject(parameterIndex, value);
                            }
                        });

                columns.add(new ColumnInfo(columnName, mysqlType, gaussType, converter));
            }
        }

        return columns;
    }
}
