package xogta.tech;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class ChangeDetector {
    private HiveMetastoreManager sourceMetastore;
    private DatabaseManager dbManager;

    public ChangeDetector(HiveMetastoreManager sourceMetastore, DatabaseManager dbManager) {
        this.sourceMetastore = sourceMetastore;
        this.dbManager = dbManager;
    }

    public Map<String, List<String>> detectChanges() throws SQLException {
        Timestamp lastReplicationTime = dbManager.getLastReplicationTime();
        Map<String, List<String>> changedTables = sourceMetastore.getChangedTables(lastReplicationTime);
        List<String> includedDatabases = dbManager.getIncludedDatabases();
        
        return changedTables.entrySet().stream()
            .filter(entry -> includedDatabases.contains(entry.getKey()))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    String database = entry.getKey();
                    List<String> excludedTables;
                    try {
                        excludedTables = dbManager.getExcludedTables(database);
                    } catch (SQLException e) {
                        // Log the error and continue with an empty list of excluded tables
                        System.err.println("Error fetching excluded tables for database " + database + ": " + e.getMessage());
                        excludedTables = Collections.emptyList();
                    }
                    final List<String> finalExcludedTables = excludedTables;
                    return entry.getValue().stream()
                        .filter(table -> !finalExcludedTables.contains(table))
                        .collect(Collectors.toList());
                }
            ));
    }
}