package xogta.tech;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseManager {
    private Connection connection;

    public DatabaseManager(ConfigManager config) throws SQLException {
        String url = config.getProperty("metadata.jdbc.url");
        String user = config.getProperty("metadata.jdbc.user");
        String password = config.getProperty("metadata.jdbc.password");
        connection = DriverManager.getConnection(url, user, password);
    }

    public void initReplicationTrack(String database, String table) throws SQLException {
        String sql = "INSERT INTO replication_tracking (database_name, table_name, replication_start) VALUES (?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, database);
            pstmt.setString(2, table);
            pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            pstmt.executeUpdate();
        }
    }

    public void updateReplicationStatus(String database, String table, String step, String status) throws SQLException {
        String columnName = step.toLowerCase().replace(" ", "_") + "_status";
        String timestampColumn = step.toLowerCase().replace(" ", "_") + "_timestamp";
        String sql = "UPDATE replication_tracking SET " + columnName + " = ?, " + timestampColumn + " = ? " +
                     "WHERE database_name = ? AND table_name = ? AND replication_end IS NULL";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, status);
            pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            pstmt.setString(3, database);
            pstmt.setString(4, table);
            pstmt.executeUpdate();
        }
    }

    public void finalizeReplicationTrack(String database, String table, String finalStatus) throws SQLException {
        String sql = "UPDATE replication_tracking SET replication_status = ?, replication_end = ? " +
                     "WHERE database_name = ? AND table_name = ? AND replication_end IS NULL";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, finalStatus);
            pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            pstmt.setString(3, database);
            pstmt.setString(4, table);
            pstmt.executeUpdate();
        }
    }

    public List<String> getExcludedTables(String database) throws SQLException {
        List<String> excludedTables = new ArrayList<>();
        String sql = "SELECT table_name FROM excluded_tables WHERE database_name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, database);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    excludedTables.add(rs.getString("table_name"));
                }
            }
        }
        return excludedTables;
    }

    public List<String> getIncludedDatabases() throws SQLException {
        List<String> includedDatabases = new ArrayList<>();
        String sql = "SELECT database_name FROM included_databases";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                includedDatabases.add(rs.getString("database_name"));
            }
        }
        return includedDatabases;
    }

    public Timestamp getLastReplicationTime() throws SQLException {
        String sql = "SELECT MAX(replication_end) as last_replication FROM replication_tracking WHERE replication_status = 'COMPLETED'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getTimestamp("last_replication");
            }
        }
        return new Timestamp(0);
    }
}