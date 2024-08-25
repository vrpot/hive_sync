package xogta.tech;

import java.util.Map;
import java.util.List;

public class HiveReplicationJob {
    public static void main(String[] args) {
        try {
            KerberosAuthenticator.authenticate();
            ConfigManager config = new ConfigManager("config.properties");
            DatabaseManager dbManager = new DatabaseManager(config);
            HiveMetastoreManager sourceMetastore = new HiveMetastoreManager(config, "source");
            HiveMetastoreManager targetMetastore = new HiveMetastoreManager(config, "target");
            ChangeDetector changeDetector = new ChangeDetector(sourceMetastore, dbManager);
            
            int maxTotalThreads = Integer.parseInt(config.getProperty("max.total.threads"));
            int maxThreadsPerDb = Integer.parseInt(config.getProperty("max.threads.per.db"));
            
            Replicator replicator = new Replicator(sourceMetastore, targetMetastore, dbManager, maxTotalThreads, maxThreadsPerDb);

            Map<String, List<String>> changedTables = changeDetector.detectChanges();
            replicator.replicateDatabases(changedTables);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}