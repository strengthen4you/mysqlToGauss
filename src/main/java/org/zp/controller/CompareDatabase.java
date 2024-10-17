package org.zp.controller;

import org.zp.conf.DatabaseConfig;
import org.zp.entity.DataBaseInfo;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.sql.*;
import java.util.*;

/**
 * @Author zp
 * @Date 2024/9/20 17:47
 */

@RestController
@RequestMapping("/compare")
public class CompareDatabase {

    @Autowired
    private DatabaseConfig databaseConfig;


    @GetMapping("/getDifference")
    public void downloadExcel(HttpServletResponse response) {

        DataBaseInfo mysqlDataBaseInfo = databaseConfig.getMysqlDataBaseInfo();
        DataBaseInfo gaussDataBaseInfo = databaseConfig.getGaussDataBaseInfo();

        try {
            DatabaseMetaData mysqlMetaData = getConnection(mysqlDataBaseInfo.getUrl(), mysqlDataBaseInfo.getUser(), mysqlDataBaseInfo.getPassword()).getMetaData();
            DatabaseMetaData gaussMetaData = getConnection(gaussDataBaseInfo.getUrl(), gaussDataBaseInfo.getUser(), gaussDataBaseInfo.getPassword()).getMetaData();

            List<String> tables = getTables(mysqlMetaData);
            Workbook workbook = new XSSFWorkbook();
            Sheet sheet = workbook.createSheet("Database Comparison");

            createHeader(sheet);

            int rowNum = 1;
            for (String table : tables) {
                Map<String, Object> mysqlInfo = getTableInfo(mysqlMetaData, table, mysqlDataBaseInfo.getSchema());
                Map<String, Object> gaussInfo = getTableInfo(gaussMetaData, table, gaussDataBaseInfo.getSchema());
                createRow(sheet, rowNum++, table, mysqlInfo, gaussInfo);
            }

            response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            response.setHeader("Content-Disposition", "attachment; filename=diff.xlsx");

            workbook.write(response.getOutputStream());
            workbook.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private static Connection getConnection(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }


    private static List<String> getTables(DatabaseMetaData metaData) throws SQLException {
        List<String> tables = new ArrayList<>();
        ResultSet rs = metaData.getTables(null, null, "%", new String[]{"TABLE"});
        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"));
        }
        return tables;
    }

    private static Map<String, Object> getTableInfo(DatabaseMetaData metaData, String tableName, String schema) throws SQLException {
        Map<String, Object> info = new HashMap<>();

        // Get row count
        try (Statement stmt = metaData.getConnection().createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
            if (rs.next()) {
                info.put("rowCount", rs.getLong(1));
            }
        }

        // Get primary keys
        List<String> primaryKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(null, schema, tableName)) {
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        }
        info.put("primaryKeys", String.join(", ", primaryKeys));

        // Get indexes
        Set<String> indexes = new HashSet<>();
        try (ResultSet rs = metaData.getIndexInfo(null, schema, tableName, false, false)) {
            while (rs.next()) {
                indexes.add(rs.getString("INDEX_NAME"));
            }
        }
        info.put("indexes", String.join(", ", indexes));

        // Get foreign keys
        List<String> foreignKeys = new ArrayList<>();
        try (ResultSet rs = metaData.getImportedKeys(null, schema, tableName)) {
            while (rs.next()) {
                foreignKeys.add(rs.getString("FKCOLUMN_NAME") + " -> " +
                        rs.getString("PKTABLE_NAME") + "." + rs.getString("PKCOLUMN_NAME"));
            }
        }
        info.put("foreignKeys", String.join(", ", foreignKeys));

        return info;
    }

    private static void createHeader(Sheet sheet) {
        Row headerRow = sheet.createRow(0);
        headerRow.createCell(0).setCellValue("Table Name");
        headerRow.createCell(1).setCellValue("MySQL Row Count");
        headerRow.createCell(2).setCellValue("Gauss Row Count");
        headerRow.createCell(3).setCellValue("MySQL Primary Keys");
        headerRow.createCell(4).setCellValue("Gauss Primary Keys");
        headerRow.createCell(5).setCellValue("MySQL Indexes");
        headerRow.createCell(6).setCellValue("Gauss Indexes");
        headerRow.createCell(7).setCellValue("MySQL Foreign Keys");
        headerRow.createCell(8).setCellValue("Gauss Foreign Keys");
    }

    private static void createRow(Sheet sheet, int rowNum, String tableName,
                                  Map<String, Object> mysqlInfo, Map<String, Object> gaussInfo) {
        Row row = sheet.createRow(rowNum);
        row.createCell(0).setCellValue(tableName);

        createComparisonCells(row, 1, mysqlInfo.get("rowCount"), gaussInfo.get("rowCount"));
        createComparisonCells(row, 3, mysqlInfo.get("primaryKeys"), gaussInfo.get("primaryKeys"));
        createComparisonCells(row, 5, mysqlInfo.get("indexes"), gaussInfo.get("indexes"));
        createComparisonCells(row, 7, mysqlInfo.get("foreignKeys"), gaussInfo.get("foreignKeys"));
    }

    private static void createComparisonCells(Row row, int startColumn, Object mysqlValue, Object gaussValue) {
        Cell mysqlCell = row.createCell(startColumn);
        Cell gaussCell = row.createCell(startColumn + 1);

        String mysqlV = mysqlValue.toString();
        String gaussV = gaussValue.toString();

        mysqlCell.setCellValue(mysqlV);
        gaussCell.setCellValue(gaussV);

        if (!(mysqlV.trim().length() == gaussV.trim().length())) {
            CellStyle style = row.getSheet().getWorkbook().createCellStyle();
            style.setFillForegroundColor(IndexedColors.YELLOW.getIndex());
            style.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            mysqlCell.setCellStyle(style);
            gaussCell.setCellStyle(style);
        }
    }




}
