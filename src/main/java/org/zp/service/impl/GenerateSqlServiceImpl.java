package org.zp.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zp.entity.*;
import org.springframework.stereotype.Service;
import org.zp.service.GenerateSqlService;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author zp
 * @Date 2024/10/12 18:05
 */

@Service
public class GenerateSqlServiceImpl implements GenerateSqlService {

    private static final Logger logger = LoggerFactory.getLogger(GenerateSqlServiceImpl.class);

    @Override
    public boolean generateSql(DataBaseInfo dataBaseInfo, String taskId) {
        logger.info("开始生成SQL文件，数据库信息: {}", dataBaseInfo.getSchema());
        ProcessInfo processInfo = new ProcessInfo(1.00, ProcessInfo.TaskStatus.RUNNING, ProcessInfo.TaskType.GENERATE_SQL);
        try (Connection conn = DriverManager.getConnection(dataBaseInfo.getUrl(), dataBaseInfo.getUser(), dataBaseInfo.getPassword())) {
            DatabaseMetaData metaData = conn.getMetaData();
            ProcessInfo.taskProgressMap.put(taskId, processInfo);
            generateTableStructure(conn, metaData, dataBaseInfo.getSchema() + SqlFile.STRUCTURE.getFileName());
            processInfo.setProcess(6.00);
            ProcessInfo.taskProgressMap.put(taskId, processInfo);
            generateIndexes(metaData, dataBaseInfo.getSchema() + SqlFile.INDEXES.getFileName());
            generateForeignKeys(metaData, dataBaseInfo.getSchema() + SqlFile.FOREIGN_KEYS.getFileName());
            generateCheckConstraints(conn, dataBaseInfo.getSchema() + SqlFile.CHECK_CONSTRAINTS.getFileName());
            generateViews(conn, dataBaseInfo.getSchema() + SqlFile.VIEWS.getFileName());
            processInfo.setProcess(10.00);
            ProcessInfo.taskProgressMap.put(taskId, processInfo);
            logger.info("SQL文件生成成功");
            return true;
        } catch (SQLException | IOException e) {
            processInfo.setTaskStatus(ProcessInfo.TaskStatus.FAIL);
            ProcessInfo.taskProgressMap.put(taskId, processInfo);
            logger.error("生成SQL文件失败", e);
            return false;
        }
    }


    private void generateTableStructure(Connection conn, DatabaseMetaData metaData, String fileName) throws SQLException, IOException {
        logger.info("开始生成表结构，文件名: {}", fileName);
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                writer.println("CREATE TABLE " + tableName + " (");

                ResultSet columns = metaData.getColumns(null, null, tableName, "%");
                boolean firstColumn = true;
                while (columns.next()) {
                    if (!firstColumn) {
                        writer.println(",");
                    }
                    String columnName = columns.getString("COLUMN_NAME");
                    String dataType = columns.getString("TYPE_NAME");
                    int columnSize = columns.getInt("COLUMN_SIZE");

                    writer.print("    " + columnName + " ");
                    String mappedDataType = DataTypeMappings.getDataTypeMappings().getOrDefault(dataType.toUpperCase(), dataType);
                    if (mappedDataType.contains("%d")) {
                        mappedDataType = String.format(mappedDataType, "VARCHAR".equalsIgnoreCase(dataType) ? columnSize * 3 : columnSize);
                    }
                    writer.print(mappedDataType);

                    String nullableStr = columns.getString("IS_NULLABLE");
                    if ("NO".equals(nullableStr)) {
                        writer.print(" NOT NULL");
                    }

                    // 添加自增长信息
                    boolean isAutoIncrement = columns.getBoolean("IS_AUTOINCREMENT");
                    if (isAutoIncrement) {
                        writer.print(" AUTO_INCREMENT");
                    }

                    // 添加默认值信息
                    String defaultValue = columns.getString("COLUMN_DEF");
                    if (defaultValue != null) {
                        if (DefaultFunctionMappings.getMysqlToGaussDefaultFunctions().containsKey(defaultValue)) {
                            writer.print(" DEFAULT " + DefaultFunctionMappings.convertDefaultFunction(defaultValue));
                        } else {
                            writer.print(" DEFAULT " + "'" + defaultValue + "'");
                        }
                    }

                    String columnComment = columns.getString("REMARKS");
                    if (columnComment != null && !columnComment.isEmpty()) {
                        writer.print(" COMMENT '" + columnComment.replace("'", "''") + "'");
                    }

                    firstColumn = false;
                }

                // 添加主键信息
                ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);
                List<String> pkColumns = new ArrayList<>();
                while (primaryKeys.next()) {
                    pkColumns.add(primaryKeys.getString("COLUMN_NAME"));
                }
                if (!pkColumns.isEmpty()) {
                    writer.println(",");
                    writer.println("    PRIMARY KEY (" + String.join(", ", pkColumns) + ")");
                }

                writer.println(")");

                // 添加表注释
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SHOW TABLE STATUS WHERE Name = '" + tableName + "'");
                    if (rs.next()) {
                        String tableComment = rs.getString("Comment");
                        if (tableComment != null && !tableComment.isEmpty()) {
                            writer.println("COMMENT = '" + tableComment.replace("'", "''") + "'");
                        }
                    }
                }

                writer.println(";");
                writer.println();
            }
        }
        logger.info("表结构生成完成");
    }

    private void generateIndexes(DatabaseMetaData metaData, String fileName) throws SQLException, IOException {
        logger.info("开始生成索引，文件名: {}", fileName);
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                ResultSet indexes = metaData.getIndexInfo(null, null, tableName, false, false);

                Map<String, List<IndexInfo>> indexMap = new HashMap<>();
                while (indexes.next()) {
                    String indexName = indexes.getString("INDEX_NAME");
                    if (indexName != null && !"PRIMARY".equals(indexName)) {
                        String columnName = indexes.getString("COLUMN_NAME");
                        boolean nonUnique = indexes.getBoolean("NON_UNIQUE");
                        indexMap.computeIfAbsent(indexName, k -> new ArrayList<>())
                                .add(new IndexInfo(columnName, !nonUnique));
                    }
                }

                for (Map.Entry<String, List<IndexInfo>> entry : indexMap.entrySet()) {
                    boolean isUnique = entry.getValue().stream().allMatch(IndexInfo::isUnique);
                    writer.print(isUnique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ");
                    writer.println(entry.getKey() + " ON " + tableName + " (" +
                            entry.getValue().stream().map(IndexInfo::getColumnName).collect(Collectors.joining(", ")) + ");");
                }
                writer.println();
            }
        }
        logger.info("索引生成完成");
    }

    private void generateForeignKeys(DatabaseMetaData metaData, String fileName) throws SQLException, IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                ResultSet foreignKeys = metaData.getImportedKeys(null, null, tableName);

                Map<String, List<ForeignKeyInfo>> fkMap = new HashMap<>();
                while (foreignKeys.next()) {
                    String fkName = foreignKeys.getString("FK_NAME");
                    String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                    String pkTableName = foreignKeys.getString("PKTABLE_NAME");
                    String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
                    short updateRule = foreignKeys.getShort("UPDATE_RULE");
                    short deleteRule = foreignKeys.getShort("DELETE_RULE");

                    fkMap.computeIfAbsent(fkName, k -> new ArrayList<>())
                            .add(new ForeignKeyInfo(fkColumnName, pkTableName, pkColumnName, updateRule, deleteRule));
                }

                for (Map.Entry<String, List<ForeignKeyInfo>> entry : fkMap.entrySet()) {
                    writer.println("ALTER TABLE " + tableName + " ADD CONSTRAINT " + entry.getKey() +
                            " FOREIGN KEY (" +
                            entry.getValue().stream().map(ForeignKeyInfo::getFkColumnName).collect(Collectors.joining(", ")) +
                            ") REFERENCES " + entry.getValue().get(0).getPkTableName() + " (" +
                            entry.getValue().stream().map(ForeignKeyInfo::getPkColumnName).collect(Collectors.joining(", ")) +
                            ")");

                    // 添加级联规则
                    ForeignKeyInfo firstFk = entry.getValue().get(0);
                    String updateRule = getCascadeRule(firstFk.getUpdateRule(), "UPDATE");
                    String deleteRule = getCascadeRule(firstFk.getDeleteRule(), "DELETE");

                    List<String> rules = new ArrayList<>();
                    if (!updateRule.isEmpty()) {
                        rules.add(updateRule);
                    }
                    if (!deleteRule.isEmpty()) {
                        rules.add(deleteRule);
                    }

                    if (!rules.isEmpty()) {
                        writer.println("    " + String.join(" ", rules));
                    }

                    writer.println(";");
                }
                writer.println();
            }
        }
    }

    private String getCascadeRule(short rule, String action) {
        switch (rule) {
            case DatabaseMetaData.importedKeyCascade:
                return "ON " + action + " CASCADE";
            case DatabaseMetaData.importedKeySetNull:
                return "ON " + action + " SET NULL";
            case DatabaseMetaData.importedKeySetDefault:
                return "ON " + action + " SET DEFAULT";
            case DatabaseMetaData.importedKeyRestrict:
                return "ON " + action + " RESTRICT";
            case DatabaseMetaData.importedKeyNoAction:
                return "ON " + action + " NO ACTION";
            default:
                return "";
        }
    }

    private void generateCheckConstraints(Connection conn, String fileName) throws SQLException, IOException {
        logger.info("开始生成检查约束，文件名: {}", fileName);
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SHOW CREATE TABLE " + tableName);
                    if (rs.next()) {
                        String createTableSql = rs.getString(2);
                        List<String> checkConstraints = extractCheckConstraints(createTableSql);
                        for (String constraint : checkConstraints) {
                            writer.println("ALTER TABLE " + tableName + " ADD " + constraint + ";");
                        }
                    }
                }
            }
        }
        logger.info("检查约束生成完成");
    }

    private List<String> extractCheckConstraints(String createTableSql) {
        List<String> constraints = new ArrayList<>();
        String[] lines = createTableSql.split("\n");
        for (String line : lines) {
            line = line.trim();
            if (line.startsWith("CONSTRAINT") && line.contains("CHECK")) {
                constraints.add(line.replaceAll(",$", ""));
            }
        }
        return constraints;
    }

    private void generateViews(Connection conn, String fileName) throws SQLException, IOException {
        logger.info("开始生成视图，文件名: {}", fileName);
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet views = metaData.getTables(null, null, "%", new String[]{"VIEW"});

            while (views.next()) {
                String viewName = views.getString("TABLE_NAME");
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery("SHOW CREATE VIEW " + viewName);
                    if (rs.next()) {
                        String createViewSql = rs.getString(2);
                        String viewDefinition = extractViewDefinition(createViewSql);
                        String formattedCreateView = String.format("CREATE VIEW %s AS %s;", viewName, viewDefinition);
                        writer.println(formattedCreateView.replace("`", ""));
                        writer.println();
                    }
                }
            }
        }
        logger.info("视图生成完成");
    }

    private String extractViewDefinition(String createViewSql) {
        int asSelectIndex = createViewSql.toUpperCase().indexOf("AS SELECT");
        if (asSelectIndex != -1) {
            return createViewSql.substring(asSelectIndex + 3).trim();
        }
        return createViewSql;
    }

    private boolean matchIntExpression(String input) {
        String pattern = "int\\((100|[1-9][0-9]?)\\)";
        return input.matches(pattern);
    }

    private boolean matchTinyintExpression(String input) {
        String pattern = "tinyint\\((100|[1-9][0-9]?)\\)";
        return input.matches(pattern);
    }
}
