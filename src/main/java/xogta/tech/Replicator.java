package xogta.tech;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class Replicator {
    private final HiveMetastoreManager sourceMetastore;
    private final HiveMetastoreManager targetMetastore;
    private final DatabaseManager dbManager;
    private final int MAX_TOTAL_THREADS;
    private final int MAX_THREADS_PER_DB;

    public Replicator(HiveMetastoreManager sourceMetastore, 
                      HiveMetastoreManager targetMetastore, DatabaseManager dbManager, 
                      int maxTotalThreads, int maxThreadsPerDb) {
        this.sourceMetastore = Objects.requireNonNull(sourceMetastore, "Source HiveMetastoreManager cannot be null");
        this.targetMetastore = Objects.requireNonNull(targetMetastore, "Target HiveMetastoreManager cannot be null");
        this.dbManager = Objects.requireNonNull(dbManager, "DatabaseManager cannot be null");
        this.MAX_TOTAL_THREADS = maxTotalThreads;
        this.MAX_THREADS_PER_DB = maxThreadsPerDb;
    }

    public void replicateDatabases(Map<String, List<String>> changedTables) {
        ExecutorService dbExecutor = Executors.newFixedThreadPool(Math.min(changedTables.size(), MAX_TOTAL_THREADS));
        List<Future<?>> dbFutures = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : changedTables.entrySet()) {
            String database = entry.getKey();
            List<String> tables = entry.getValue();
            dbFutures.add(dbExecutor.submit(() -> replicateDatabase(database, tables)));
        }

        for (Future<?> future : dbFutures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        dbExecutor.shutdown();
    }

    private void replicateDatabase(String database, List<String> tables) {
        ExecutorService tableExecutor = Executors.newFixedThreadPool(Math.min(tables.size(), MAX_THREADS_PER_DB));
        List<Future<?>> tableFutures = new ArrayList<>();

        for (String table : tables) {
            tableFutures.add(tableExecutor.submit(() -> replicateTable(database, table)));
        }

        for (Future<?> future : tableFutures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        tableExecutor.shutdown();
    }

    private void replicateTable(String database, String table) {
        try {
            String sourceLocation = sourceMetastore.getTableLocation(database + "." + table);
            String targetLocation = targetMetastore.getTableLocation(database + "." + table);
            Path sourcePath = new Path(sourceLocation);
            Path targetPath = new Path(targetLocation);
            
            dbManager.initReplicationTrack(database, table);
            
            String tableDDL = sourceMetastore.getTableDDL(database + "." + table);
            targetMetastore.dropTable(database + "." + table);
            targetMetastore.createTable(tableDDL);
            
            dbManager.updateReplicationStatus(database, table, "Schema Replication", "COMPLETED");

            Configuration conf = new Configuration();
            DistCpOptions options = new DistCpOptions.Builder(Collections.singletonList(sourcePath), targetPath)
                .withSyncFolder(true)
                .withOverwrite(true)
                .build();

            new DistCp(conf, options).execute();
            
            targetMetastore.repairTable(database + "." + table);
            dbManager.updateReplicationStatus(database, table, "Data Replication", "COMPLETED");
            dbManager.finalizeReplicationTrack(database, table, "COMPLETED");
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                dbManager.updateReplicationStatus(database, table, "Replication", "FAILED");
                dbManager.finalizeReplicationTrack(database, table, "FAILED");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                dbManager.updateReplicationStatus(database, table, "Replication", "FAILED");
                dbManager.finalizeReplicationTrack(database, table, "FAILED");
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }
}