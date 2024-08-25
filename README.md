# Hive Replication Tool

Noticed several issues while using Cloudera BDR. When multiple databases are being replicated, we have to chose snapshot creation at DB level and it is not ideal when we have many DB's. This project is an attempt to implement a near real-time replication tool for Apache Hive, designed as an alternative to Cloudera BDR for CDP 7.1.9. It focuses on replicating external Hive tables between two clusters running the same version of CDP.

## Components

1. **ConfigManager**: Manages configuration properties loaded from a file.
2. **DatabaseManager**: Handles database operations for tracking replication status and metadata.
3. **HiveMetastoreManager**: Interacts with Hive metastore to perform table operations and fetch metadata.
4. **ChangeDetector**: Identifies tables that have changed since the last replication.
5. **Replicator**: Orchestrates the replication process for databases and tables.
6. **HiveReplicationJob**: The main class that initiates and runs the replication process.
7. **KerberosAuthenticator**: Handles Kerberos authentication (implementation required).

## Features

- Near real-time replication of Hive external tables
- Parallel replication of databases and tables
- Configurable thread pools for optimized performance
- Kerberos authentication support
- Tracking of replication status and history
- Exclusion of specific tables from replication
- Error handling and logging

## Prerequisites

- Java 8 or higher
- Apache Hadoop libraries
- Apache Hive libraries
- MySQL Connector for Java
- Properly configured Kerberos authentication (if using)

## Configuration

Create a `config.properties` file with the following properties:

```properties
# Source Hive JDBC URL
source.hive.jdbc.url=jdbc:hive2://source-hive-server:10000/default;principal=hive/source-hive-server@REALM

# Target Hive JDBC URL
target.hive.jdbc.url=jdbc:hive2://target-hive-server:10000/default;principal=hive/target-hive-server@REALM

# Metadata database JDBC URL
metadata.jdbc.url=jdbc:mysql://metadata-db-server:3306/replication_metadata

# Metadata database credentials
metadata.jdbc.user=replication_user
metadata.jdbc.password=password

# Replication thread pool sizes
max.total.threads=10
max.threads.per.db=5

# Other configuration properties as needed

Usage

Compile the project and create a JAR file.
Set up the config.properties file with appropriate values.
Run the HiveReplicationJob class:

java -cp hive-replication-tool.jar:path/to/dependencies/* xogta.tech.HiveReplicationJob

Implementation Notes

Implement the KerberosAuthenticator class based on your specific Kerberos setup.
Ensure that the necessary database tables are created in the metadata database for tracking replication status and history.
Properly configure Kerberos and ensure that keytabs are available if using Kerberos authentication.
Test thoroughly in a non-production environment before deploying to production clusters.

Limitations

This tool is designed for CDP 7.1.9 and may require modifications for other versions.
It focuses on external Hive tables and may not support all Hive table types.
The tool assumes that both source and target clusters are running the same CDP version.

Contributing
Contributions to improve the tool or fix issues are welcome. Please submit pull requests or open issues on the project repository.
