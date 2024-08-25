package xogta.tech;

import java.sql.*;
import java.util.*;

public class HiveMetastoreManager {
    private Connection connection;

    public HiveMetastoreManager(ConfigManager config, String instance) throws SQLException {
        String url = config.getProperty(instance + ".hive.jdbc.url");
        String user = config.getProperty(instance + ".hive.jdbc.user");
        String password = config.getProperty(instance + ".hive.jdbc.password");
        connection = DriverManager.getConnection(url, user, password);
    }

    public Map<String, List<String>> getChangedTables(Timestamp lastReplicationTime) throws SQLException {
        Map<String, List<String>> changedTables = new HashMap<>();
        String sql = "SELECT d.NAME as db_name, t.TBL_NAME as tbl_name " +
                     "FROM DBS d JOIN TBLS t ON d.DB_ID = t.DB_ID " +
                     "WHERE t.LAST_ACCESS_TIME > ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setTimestamp(1, lastReplicationTime);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String dbName = rs.getString("db_name");
                    String tblName = rs.getString("tbl_name");
                    changedTables.computeIfAbsent(dbName, k -> new ArrayList<>()).add(tblName);
                }
            }
        }
        return changedTables;
    }

    public void createTable(String tableDDL) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(tableDDL);
        }
    }

    public void dropTable(String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public void addPartition(String tableName, String partitionSpec) throws SQLException {
        String sql = "ALTER TABLE " + tableName + " ADD IF NOT EXISTS PARTITION (" + partitionSpec + ")";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public void repairTable(String tableName) throws SQLException {
        String sql = "MSCK REPAIR TABLE " + tableName;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public String getTableDDL(String tableName) throws SQLException {
        String sql = "SHOW CREATE TABLE " + tableName;
        StringBuilder ddl = new StringBuilder();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ddl.append(rs.getString(1)).append("\n");
            }
        }
        return ddl.toString();
    }

    public String getTableLocation(String tableName) throws SQLException {
        String sql = "DESCRIBE FORMATTED " + tableName;
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                if (rs.getString(1).trim().equals("Location:")) {
                    return rs.getString(2).trim();
                }
            }
        }
        throw new SQLException("Table location not found for " + tableName);
    }
}