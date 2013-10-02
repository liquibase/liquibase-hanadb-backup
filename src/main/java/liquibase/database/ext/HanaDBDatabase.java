package liquibase.database.ext;


import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Relation;

import java.util.HashSet;
import java.util.Set;


public class HanaDBDatabase extends AbstractJdbcDatabase {

    public static final String PRODUCT_NAME = "HDB";
    protected Set<String> systemViews = new HashSet<String>();

    public HanaDBDatabase() {
        super();
        systemViews.add("---");

        systemViews.add("AUDIT_POLICIES");
        systemViews.add("AUTHORIZATION_GRAPH");
        systemViews.add("CONSTRAINTS");
        systemViews.add("CS_BO_VIEWS");
        systemViews.add("CS_FREESTYLE_COLUMNS");
        systemViews.add("CS_JOIN_CONDITIONS");
        systemViews.add("CS_JOIN_CONSTRAINTS");
        systemViews.add("CS_JOIN_PATHS");
        systemViews.add("CS_JOIN_TABLES");
        systemViews.add("CS_KEY_FIGURES");
        systemViews.add("CS_VIEW_COLUMNS");
        systemViews.add("DATA_TYPES");
        systemViews.add("EFFECTIVE_PRIVILEGES");
        systemViews.add("EXPLAIN_PLAN_TABLE");
        systemViews.add("FULLTEXT_INDEXES");
        systemViews.add("FUNCTIONS");
        systemViews.add("FUNCTION_PARAMETERS");
        systemViews.add("GRANTED_PRIVILEGES");
        systemViews.add("GRANTED_ROLES");
        systemViews.add("INDEXES");
        systemViews.add("INDEX_COLUMNS");
        systemViews.add("INVALID_CONNECT_ATTEMPTS");
        systemViews.add("M_ATTACHED_STORAGES");
        systemViews.add("M_BACKUP_CATALOG");
        systemViews.add("M_BACKUP_CATALOG_FILES");
        systemViews.add("M_BACKUP_CONFIGURATION");
        systemViews.add("M_BLOCKED_TRANSACTIONS");
        systemViews.add("M_CACHES");
        systemViews.add("M_CACHES_RESET");
        systemViews.add("M_CACHE_ENTRIES");
        systemViews.add("M_CATALOG_MEMORY");
        systemViews.add("M_CE_CALCSCENARIOS");
        systemViews.add("M_CE_CALCVIEW_DEPENDENCIES");
        systemViews.add("M_CE_DEBUG_INFOS");
        systemViews.add("M_CE_DEBUG_JSONS");
        systemViews.add("M_CE_DEBUG_NODE_MAPPING");
        systemViews.add("M_CE_PLE_CALCSCENARIOS");
        systemViews.add("M_CLIENT_VERSIONS");
        systemViews.add("M_COMPACTION_THREAD");
        systemViews.add("M_CONDITIONAL_VARIABLES");
        systemViews.add("M_CONDITIONAL_VARIABLES_RESET");
        systemViews.add("M_CONFIGURATION");
        systemViews.add("M_CONNECTIONS");
        systemViews.add("M_CONNECTION_STATISTICS");
        systemViews.add("M_CONTAINER_DIRECTORY");
        systemViews.add("M_CONTAINER_NAME_DIRECTORY");
        systemViews.add("M_CONTEXT_MEMORY");
        systemViews.add("M_CONTEXT_MEMORY_RESET");
        systemViews.add("M_CONVERTER_STATISTICS");
        systemViews.add("M_CONVERTER_STATISTICS_RESET");
        systemViews.add("M_CS_ALL_COLUMNS");
        systemViews.add("M_CS_COLUMNS");
        systemViews.add("M_CS_PARTITIONS");
        systemViews.add("M_CS_TABLES");
        systemViews.add("M_CS_UNLOADS");
        systemViews.add("M_DATABASE");
        systemViews.add("M_DATABASE_HISTORY");
        systemViews.add("M_DATA_VOLUMES");
        systemViews.add("M_DATA_VOLUME_PAGE_STATISTICS");
        systemViews.add("M_DATA_VOLUME_PAGE_STATISTICS_RESET");
        systemViews.add("M_DATA_VOLUME_SUPERBLOCK_STATISTICS");
        systemViews.add("M_DELTA_MERGE_STATISTICS");
        systemViews.add("M_DISKS");
        systemViews.add("M_ERROR_CODES");
        systemViews.add("M_EVENTS");
        systemViews.add("M_EXPENSIVE_STATEMENTS");
        systemViews.add("M_EXPORT_BINARY_STATUS");
        systemViews.add("M_EXTRACTORS");
        systemViews.add("M_FEATURES");
        systemViews.add("M_FULLTEXT_QUEUES");
        systemViews.add("M_GARBAGE_COLLECTION_STATISTICS");
        systemViews.add("M_GARBAGE_COLLECTION_STATISTICS_RESET");
        systemViews.add("M_HEAP_MEMORY");
        systemViews.add("M_HEAP_MEMORY_RESET");
        systemViews.add("M_HISTORY_INDEX_LAST_COMMIT_ID");
        systemViews.add("M_HOST_INFORMATION");
        systemViews.add("M_HOST_RESOURCE_UTILIZATION");
        systemViews.add("M_IMPORT_BINARY_STATUS");
        systemViews.add("M_INIFILES");
        systemViews.add("M_INIFILE_CONTENTS");
        systemViews.add("M_JOB_PROGRESS");
        systemViews.add("M_LANDSCAPE_HOST_CONFIGURATION");
        systemViews.add("M_LICENSE");
        systemViews.add("M_LICENSE_USAGE_HISTORY");
        systemViews.add("M_LIVECACHE_CONTAINER_STATISTICS");
        systemViews.add("M_LIVECACHE_CONTAINER_STATISTICS_RESET");
        systemViews.add("M_LIVECACHE_LOCKS");
        systemViews.add("M_LIVECACHE_LOCK_STATISTICS");
        systemViews.add("M_LIVECACHE_LOCK_STATISTICS_RESET");
        systemViews.add("M_LIVECACHE_OMS_VERSIONS");
        systemViews.add("M_LIVECACHE_PROCEDURE_STATISTICS");
        systemViews.add("M_LIVECACHE_PROCEDURE_STATISTICS_RESET");
        systemViews.add("M_LIVECACHE_SCHEMA_STATISTICS");
        systemViews.add("M_LIVECACHE_SCHEMA_STATISTICS_RESET");
        systemViews.add("M_LOCK_WAITS_STATISTICS");
        systemViews.add("M_LOG_BUFFERS");
        systemViews.add("M_LOG_BUFFERS_RESET");
        systemViews.add("M_LOG_PARTITIONS");
        systemViews.add("M_LOG_PARTITIONS_RESET");
        systemViews.add("M_LOG_SEGMENTS");
        systemViews.add("M_LOG_SEGMENTS_RESET");
        systemViews.add("M_MEMORY_OBJECTS");
        systemViews.add("M_MEMORY_OBJECTS_RESET");
        systemViews.add("M_MEMORY_OBJECT_DISPOSITIONS");
        systemViews.add("M_MERGED_TRACES");
        systemViews.add("M_MONITORS");
        systemViews.add("M_MONITOR_COLUMNS");
        systemViews.add("M_MUTEXES");
        systemViews.add("M_MUTEXES_RESET");
        systemViews.add("M_MVCC_TABLES");
        systemViews.add("M_OBJECT_LOCKS");
        systemViews.add("M_OBJECT_LOCK_STATISTICS");
        systemViews.add("M_OBJECT_LOCK_STATISTICS_RESET");
        systemViews.add("M_PAGEACCESS_STATISTICS");
        systemViews.add("M_PAGEACCESS_STATISTICS_RESET");
        systemViews.add("M_PASSWORD_POLICY");
        systemViews.add("M_PERFTRACE");
        systemViews.add("M_PERSISTENCE_MANAGERS");
        systemViews.add("M_PERSISTENCE_MANAGERS_RESET");
        systemViews.add("M_PREPARED_STATEMENTS");
        systemViews.add("M_READWRITELOCKS");
        systemViews.add("M_READWRITELOCKS_RESET");
        systemViews.add("M_RECORD_LOCKS");
        systemViews.add("M_REORG_ALGORITHMS");
        systemViews.add("M_REPO_TRANSPORT_FILES");
        systemViews.add("M_RS_INDEXES");
        systemViews.add("M_RS_TABLES");
        systemViews.add("M_RS_TABLE_VERSION_STATISTICS");
        systemViews.add("M_SAVEPOINTS");
        systemViews.add("M_SAVEPOINT_STATISTICS");
        systemViews.add("M_SAVEPOINT_STATISTICS_RESET");
        systemViews.add("M_SEMAPHORES");
        systemViews.add("M_SEMAPHORES_RESET");
        systemViews.add("M_SERVICES");
        systemViews.add("M_SERVICE_COMPONENT_MEMORY");
        systemViews.add("M_SERVICE_MEMORY");
        systemViews.add("M_SERVICE_NETWORK_IO");
        systemViews.add("M_SERVICE_REPLICATION");
        systemViews.add("M_SERVICE_STATISTICS");
        systemViews.add("M_SERVICE_THREADS");
        systemViews.add("M_SERVICE_THREAD_CALLSTACKS");
        systemViews.add("M_SERVICE_TRACES");
        systemViews.add("M_SERVICE_TYPES");
        systemViews.add("M_SESSION_CONTEXT");
        systemViews.add("M_SHARED_MEMORY");
        systemViews.add("M_SNAPSHOTS");
        systemViews.add("M_SQL_PLAN_CACHE");
        systemViews.add("M_SQL_PLAN_CACHE_OVERVIEW");
        systemViews.add("M_SQL_PLAN_CACHE_RESET");
        systemViews.add("M_SYSTEM_INFORMATION_STATEMENTS");
        systemViews.add("M_SYSTEM_LIMITS");
        systemViews.add("M_SYSTEM_OVERVIEW");
        systemViews.add("M_TABLES");
        systemViews.add("M_TABLE_LOB_FILES");
        systemViews.add("M_TABLE_LOCATIONS");
        systemViews.add("M_TABLE_PERSISTENCE_LOCATIONS");
        systemViews.add("M_TABLE_PERSISTENCE_STATISTICS");
        systemViews.add("M_TABLE_VIRTUAL_FILES");
        systemViews.add("M_TEMPORARY_TABLES");
        systemViews.add("M_TEMPORARY_TABLE_COLUMNS");
        systemViews.add("M_TEMPORARY_VIEWS");
        systemViews.add("M_TEMPORARY_VIEW_COLUMNS");
        systemViews.add("M_TENANTS");
        systemViews.add("M_TEXT_ANALYSIS_LANGUAGES");
        systemViews.add("M_TEXT_ANALYSIS_MIME_TYPES");
        systemViews.add("M_TOPOLOGY_TREE");
        systemViews.add("M_TRACEFILES");
        systemViews.add("M_TRACEFILE_CONTENTS");
        systemViews.add("M_TRANSACTIONS");
        systemViews.add("M_UNDO_CLEANUP_FILES");
        systemViews.add("M_VERSION_MEMORY");
        systemViews.add("M_VOLUMES");
        systemViews.add("M_VOLUME_FILES");
        systemViews.add("M_VOLUME_IO_PERFORMANCE_STATISTICS");
        systemViews.add("M_VOLUME_IO_PERFORMANCE_STATISTICS_RESET");
        systemViews.add("M_VOLUME_IO_STATISTICS");
        systemViews.add("M_VOLUME_IO_STATISTICS_RESET");
        systemViews.add("M_VOLUME_SIZES");
        systemViews.add("M_WORKLOAD");
        systemViews.add("M_XS_APPLICATIONS");
        systemViews.add("M_XS_APPLICATION_ISSUES");
        systemViews.add("OBJECTS");
        systemViews.add("OBJECT_DEPENDENCIES");
        systemViews.add("OWNERSHIP");
        systemViews.add("PRIVILEGES");
        systemViews.add("PROCEDURES");
        systemViews.add("PROCEDURE_OBJECTS");
        systemViews.add("PROCEDURE_PARAMETERS");
        systemViews.add("QUERY_PLANS");
        systemViews.add("REFERENTIAL_CONSTRAINTS");
        systemViews.add("REORG_OVERVIEW");
        systemViews.add("REORG_PLAN");
        systemViews.add("REORG_PLAN_INFOS");
        systemViews.add("REORG_STEPS");
        systemViews.add("ROLES");
        systemViews.add("SAML_PROVIDERS");
        systemViews.add("SAML_USER_MAPPINGS");
        systemViews.add("SCHEMAS");
        systemViews.add("SEQUENCES");
        systemViews.add("SQLSCRIPT_TRACE");
        systemViews.add("STATISTICS");
        systemViews.add("STRUCTURED_PRIVILEGES");
        systemViews.add("SYNONYMS");
        systemViews.add("TABLES");
        systemViews.add("TABLE_COLUMNS");
        systemViews.add("TABLE_COLUMNS_ODBC");
        systemViews.add("TABLE_GROUPS");
        systemViews.add("TRANSACTION_HISTORY");
        systemViews.add("TRIGGERS");
        systemViews.add("USERS");
        systemViews.add("USER_PARAMETERS");
        systemViews.add("VIEWS");
        systemViews.add("VIEW_COLUMNS");

        systemViews.add("GLOBAL_COLUMN_TABLES_SIZE");
        systemViews.add("GLOBAL_CPU_STATISTICS");
        systemViews.add("GLOBAL_INTERNAL_DISKFULL_EVENTS");
        systemViews.add("GLOBAL_INTERNAL_EVENTS");
        systemViews.add("GLOBAL_MEMORY_STATISTICS");
        systemViews.add("GLOBAL_PERSISTENCE_STATISTICS");
        systemViews.add("GLOBAL_TABLES_SIZE");
        systemViews.add("HOST_BLOCKED_TRANSACTIONS");
        systemViews.add("HOST_COLUMN_TABLES_PART_SIZE");
        systemViews.add("HOST_DATA_VOLUME_PAGE_STATISTICS");
        systemViews.add("HOST_DATA_VOLUME_SUPERBLOCK_STATISTICS");
        systemViews.add("HOST_DELTA_MERGE_STATISTICS");
        systemViews.add("HOST_HEAP_ALLOCATORS");
        systemViews.add("HOST_LONG_RUNNING_STATEMENTS");
        systemViews.add("HOST_MEMORY_STATISTICS");
        systemViews.add("HOST_ONE_DAY_FILE_COUNT");
        systemViews.add("HOST_RESOURCE_UTILIZATION_STATISTICS");
        systemViews.add("HOST_SERVICE_MEMORY");
        systemViews.add("HOST_SERVICE_STATISTICS");
        systemViews.add("HOST_TABLE_VIRTUAL_FILES");
        systemViews.add("HOST_VIRTUAL_FILES");
        systemViews.add("HOST_VOLUME_FILES");
        systemViews.add("HOST_VOLUME_IO_PERFORMANCE_STATISTICS");
        systemViews.add("HOST_VOLUME_IO_STATISTICS");
        systemViews.add("STATISTICS_ALERTS");
        systemViews.add("STATISTICS_ALERT_INFORMATION");
        systemViews.add("STATISTICS_ALERT_LAST_CHECK_INFORMATION");
        systemViews.add("STATISTICS_INTERVAL_INFORMATION");
        systemViews.add("STATISTICS_LASTVALUES");
        systemViews.add("STATISTICS_STATE");
        systemViews.add("STATISTICS_VERSION");
        systemViews.add("STATISTICS_ALERTS");
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "HANA DB";
    }

    @Override
    public Integer getDefaultPort() {
        return 30015;
    }

    @Override
    public Set<String> getSystemViews() {
        return systemViews;
    }


    @Override
    public String getShortName() {
        return "hanadb";
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsSequences() {
        return true;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:sapdb")) {
            return "com.sap.db.jdbc.Driver";
        }
        return null;
    }

    @Override
    public String getCurrentDateTimeFunction() {
        if (currentDateTimeFunction != null) {
            return currentDateTimeFunction;
        }

        return "CURRENT_TIMESTAMP";
    }

    @Override
    public String getDefaultSchemaName() {
        return super.getDefaultSchemaName().toUpperCase();
    }

    @Override
    public boolean isSystemObject(DatabaseObject example) {
        if (super.isSystemObject(example)) {
            return true;
        }

        if (example instanceof Relation) {
            String schemaName = example.getSchema().getName();
            if ("_SYS_SECURITY".equalsIgnoreCase(schemaName)) {
                return true;
            } else if ("_SYS_REPO".equalsIgnoreCase(schemaName)) {
                return true;
            } else if ("_SYS_STATISTICS".equalsIgnoreCase(schemaName)) {
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean supportsTablespaces() {
        return true;
    }

    @Override
    public boolean supportsAutoIncrement() {
        return false;
    }
}