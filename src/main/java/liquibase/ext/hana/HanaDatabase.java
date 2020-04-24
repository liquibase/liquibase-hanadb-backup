package liquibase.ext.hana;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import liquibase.Scope;
import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogService;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawCallStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;

public class HanaDatabase extends AbstractJdbcDatabase {

    public static final String PRODUCT_NAME = "HDB";

    protected Set<String> systemViews = new HashSet<>();
    protected Set<String> systemTables = new HashSet<>();

    public HanaDatabase() {
        setCurrentDateTimeFunction("CURRENT_TIMESTAMP");

        setObjectQuotingStrategy(ObjectQuotingStrategy.QUOTE_ONLY_RESERVED_WORDS);

        addReservedWords(getDefaultReservedWords());
        this.systemViews = getDefaultSystemViews();

        this.dateFunctions.add(new DatabaseFunction("CURRENT_DATE"));
        this.dateFunctions.add(new DatabaseFunction("CURRENT_TIME"));
        this.dateFunctions.add(new DatabaseFunction("CURRENT_UTCDATE"));
        this.dateFunctions.add(new DatabaseFunction("CURRENT_UTCTIME"));
        this.dateFunctions.add(new DatabaseFunction("CURRENT_UTCTIMESTAMP"));

        this.sequenceCurrentValueFunction = "%s.currval";
        this.sequenceNextValueFunction = "%s.nextval";

        this.unquotedObjectsAreUppercased = Boolean.TRUE;
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        if (conn instanceof JdbcConnection) {
            @SuppressWarnings("resource")
            Connection connection = ((JdbcConnection) conn).getWrappedConnection();

            if (connection == null) {
                Scope.getCurrentScope().getLog(getClass()).info("Could not get JDBC connection");
            } else {
                try {
                    addReservedWords(Arrays.asList(connection.getMetaData().getSQLKeywords().split(",\\s*")));
                } catch (SQLException e) {
                    Scope.getCurrentScope().getLog(getClass()).info("Could not get SQL keywords: " + e.getMessage());
                }

                try (PreparedStatement statement = connection
                        .prepareStatement("SELECT VIEW_NAME FROM SYS.VIEWS WHERE SCHEMA_NAME='SYS'")) {
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            this.systemViews.add(rs.getString(1));
                        }
                    }
                } catch (SQLException e) {
                    Scope.getCurrentScope().getLog(getClass()).info("Could not get system views: " + e.getMessage());
                }

                try (PreparedStatement statement = connection
                        .prepareStatement("SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME='SYS'")) {
                    try (ResultSet rs = statement.executeQuery()) {
                        while (rs.next()) {
                            this.systemTables.add(rs.getString(1));
                        }
                    }
                } catch (SQLException e) {
                    Scope.getCurrentScope().getLog(getClass()).info("Could not get system tables: " + e.getMessage());
                }
            }
        }

        super.setConnection(conn);
    }

    @Override
    public void setAutoCommit(boolean b) throws DatabaseException {
        super.setAutoCommit(b);

        if (!b && (getConnection() instanceof JdbcConnection)) {
            JdbcConnection connection = (JdbcConnection) getConnection();
            try (PreparedStatement statement = connection.prepareStatement("SET TRANSACTION AUTOCOMMIT DDL OFF")) {
                statement.executeUpdate();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Integer getDefaultPort() {
        return Integer.valueOf(30015);
    }

    @Override
    protected Set<String> getSystemTables() {
        return this.systemTables;
    }

    @Override
    public Set<String> getSystemViews() {
        return this.systemViews;
    }

    @Override
    public String getShortName() {
        return "hana";
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
        if (url.startsWith("jdbc:sap:")) {
            return "com.sap.db.jdbc.Driver";
        }
        return null;
    }

    @Override
    public boolean isSystemObject(DatabaseObject example) {
        if (super.isSystemObject(example)) {
            return true;
        }

        if (example.getSchema() != null) {
            String schemaName = correctObjectName(example.getSchema().getName(), Schema.class);
            if (schemaName != null && schemaName.startsWith("_SYS_")) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    @Override
    protected SqlStatement getConnectionSchemaNameCallStatement() {
        return new RawCallStatement("SELECT CURRENT_SCHEMA FROM SYS.DUMMY");
    }

    @Override
    public int getDataTypeMaxParameters(String dataTypeName) {
        if ("BOOLEAN".equals(dataTypeName) || "BLOB".equals(dataTypeName) || "CLOB".equals(dataTypeName)
                || "NCLOB".equals(dataTypeName) || "TEXT".equals(dataTypeName) || "BINTEXT".equals(dataTypeName)
                || "INT".equals(dataTypeName) || "INTEGER".equals(dataTypeName) || "TINYINT".equals(dataTypeName)
                || "SMALLINT".equals(dataTypeName) || "BIGINT".equals(dataTypeName)
                || "SMALLDECIMAL".equals(dataTypeName) || "REAL".equals(dataTypeName)
                || "DOUBLE".equals(dataTypeName)) {
            return 0;
        }

        if ("VARCHAR".equals(dataTypeName) || "NVARCHAR".equals(dataTypeName) || "SHORTTEXT".equals(dataTypeName)
                || "VARBINARY".equals(dataTypeName) || "FLOAT".equals(dataTypeName)) {
            return 1;
        }

        return super.getDataTypeMaxParameters(dataTypeName);
    }

    @Override
    public int getMaxFractionalDigitsForTimestamp() {
        return 7;
    }

    @Override
    public String getSystemSchema() {
        return "SYS";
    }

    @Override
    public boolean supportsDropTableCascadeConstraints() {
        return true;
    }

    @Override
    protected boolean mustQuoteObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
        boolean mustQuote = super.mustQuoteObjectName(objectName, objectType);
        if (mustQuote) {
            return mustQuote;
        }

        if (Table.class.isAssignableFrom(objectType)) {
            mustQuote = "TYPE".equals(objectName.toUpperCase(Locale.US));
        }

        return mustQuote;
    }

    private Set<String> getDefaultReservedWords() {
        /*
         * List taken from
         * https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/
         * 28bcd6af3eb6437892719f7c27a8a285.html
         */
        Set<String> reservedWords = new HashSet<>();
        reservedWords.add("ALL");
        reservedWords.add("ALTER");
        reservedWords.add("AS");
        reservedWords.add("BEFORE");
        reservedWords.add("BEGIN");
        reservedWords.add("BOTH");
        reservedWords.add("CASE");
        reservedWords.add("CHAR");
        reservedWords.add("CONDITION");
        reservedWords.add("CONNECT");
        reservedWords.add("CROSS");
        reservedWords.add("CUBE");
        reservedWords.add("CURRENT_CONNECTION");
        reservedWords.add("CURRENT_DATE");
        reservedWords.add("CURRENT_SCHEMA");
        reservedWords.add("CURRENT_TIME");
        reservedWords.add("CURRENT_TIMESTAMP");
        reservedWords.add("CURRENT_TRANSACTION_ISOLATION_LEVEL");
        reservedWords.add("CURRENT_USER");
        reservedWords.add("CURRENT_UTCDATE");
        reservedWords.add("CURRENT_UTCTIME");
        reservedWords.add("CURRENT_UTCTIMESTAMP");
        reservedWords.add("CURRVAL");
        reservedWords.add("CURSOR");
        reservedWords.add("DECLARE");
        reservedWords.add("DISTINCT");
        reservedWords.add("ELSE");
        reservedWords.add("ELSEIF");
        reservedWords.add("END");
        reservedWords.add("EXCEPT");
        reservedWords.add("EXCEPTION");
        reservedWords.add("EXEC");
        reservedWords.add("FALSE");
        reservedWords.add("FOR");
        reservedWords.add("FROM");
        reservedWords.add("FULL");
        reservedWords.add("GROUP");
        reservedWords.add("HAVING");
        reservedWords.add("IF");
        reservedWords.add("IN");
        reservedWords.add("INNER");
        reservedWords.add("INOUT");
        reservedWords.add("INTERSECT");
        reservedWords.add("INTO");
        reservedWords.add("IS");
        reservedWords.add("JOIN");
        reservedWords.add("LEADING");
        reservedWords.add("LEFT");
        reservedWords.add("LIMIT");
        reservedWords.add("LOOP");
        reservedWords.add("MINUS");
        reservedWords.add("NATURAL");
        reservedWords.add("NCHAR");
        reservedWords.add("NEXTVAL");
        reservedWords.add("NULL");
        reservedWords.add("ON");
        reservedWords.add("ORDER");
        reservedWords.add("OUT");
        reservedWords.add("PRIOR");
        reservedWords.add("RETURN");
        reservedWords.add("RETURNS");
        reservedWords.add("REVERSE");
        reservedWords.add("RIGHT");
        reservedWords.add("ROLLUP");
        reservedWords.add("ROWID");
        reservedWords.add("SELECT");
        reservedWords.add("SESSION_USER");
        reservedWords.add("SET");
        reservedWords.add("SQL");
        reservedWords.add("START");
        reservedWords.add("SYSUUID");
        reservedWords.add("TABLESAMPLE");
        reservedWords.add("TOP");
        reservedWords.add("TRAILING");
        reservedWords.add("TRUE");
        reservedWords.add("UNION");
        reservedWords.add("UNKNOWN");
        reservedWords.add("USING");
        reservedWords.add("UTCTIMESTAMP");
        reservedWords.add("VALUES");
        reservedWords.add("WHEN");
        reservedWords.add("WHERE");
        reservedWords.add("WHILE");
        reservedWords.add("WITH");
        return reservedWords;
    }

    private Set<String> getDefaultSystemViews() {
        /*
         * List taken from
         * https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/
         * 20cbb10c75191014b47ba845bfe499fe.html
         */
        Set<String> defaultSystemViews = new HashSet<>();
        defaultSystemViews.add("ABSTRACT_SQL_PLANS");
        defaultSystemViews.add("ACCESSIBLE_VIEWS");
        defaultSystemViews.add("ADAPTER_CAPABILITIES");
        defaultSystemViews.add("AFL_AREAS");
        defaultSystemViews.add("AFL_FUNCTION_PARAMETERS");
        defaultSystemViews.add("AFL_FUNCTION_PROPERTIES");
        defaultSystemViews.add("AFL_FUNCTIONS");
        defaultSystemViews.add("AFL_PACKAGES");
        defaultSystemViews.add("AFL_TEXTS");
        defaultSystemViews.add("ALL_AUDIT_LOG");
        defaultSystemViews.add("ANNOTATIONS");
        defaultSystemViews.add("APPLICATION_ENCRYPTION_KEYS");
        defaultSystemViews.add("ASSOCIATIONS");
        defaultSystemViews.add("AUDIT_ACTIONS");
        defaultSystemViews.add("AUDIT_LOG");
        defaultSystemViews.add("AUDIT_POLICIES");
        defaultSystemViews.add("AUTHORIZATION_GRAPH");
        defaultSystemViews.add("AUTHORIZATION_TYPES");
        defaultSystemViews.add("CDS_ANNOTATION_ASSIGNMENTS");
        defaultSystemViews.add("CDS_ANNOTATION_VALUES");
        defaultSystemViews.add("CDS_ARTIFACT_NAMES");
        defaultSystemViews.add("CDS_ASSOCIATIONS");
        defaultSystemViews.add("CDS_ENTITIES");
        defaultSystemViews.add("CDS_VIEWS");
        defaultSystemViews.add("CERTIFICATES");
        defaultSystemViews.add("CLIENTSIDE_ENCRYPTION_COLUMN_KEYS");
        defaultSystemViews.add("CLIENTSIDE_ENCRYPTION_KEYPAIRS");
        defaultSystemViews.add("COLUMNS");
        defaultSystemViews.add("CONSTRAINTS");
        defaultSystemViews.add("CREDENTIALS");
        defaultSystemViews.add("CS_ALL_COLUMNS");
        defaultSystemViews.add("CS_BO_VIEWS");
        defaultSystemViews.add("CS_CONCAT_COLUMNS");
        defaultSystemViews.add("CS_FREESTYLE_COLUMNS");
        defaultSystemViews.add("CS_JOIN_CONDITIONS");
        defaultSystemViews.add("CS_JOIN_CONSTRAINTS");
        defaultSystemViews.add("CS_JOIN_PATHS");
        defaultSystemViews.add("CS_JOIN_TABLES");
        defaultSystemViews.add("CS_KEY_FIGURES");
        defaultSystemViews.add("CS_VIEW_COLUMNS");
        defaultSystemViews.add("CS_VIEW_PARAMETERS");
        defaultSystemViews.add("DATA_STATISTICS");
        defaultSystemViews.add("DATA_TYPES");
        defaultSystemViews.add("DEPENDENCY_RULE_COLUMNS");
        defaultSystemViews.add("DEPENDENCY_RULES");
        defaultSystemViews.add("DYNAMIC_RESULT_CACHE");
        defaultSystemViews.add("EFFECTIVE_APPLICATION_PRIVILEGES");
        defaultSystemViews.add("EFFECTIVE_MASK_EXPRESSIONS");
        defaultSystemViews.add("EFFECTIVE_PRIVILEGE_GRANTEES");
        defaultSystemViews.add("EFFECTIVE_PRIVILEGES");
        defaultSystemViews.add("EFFECTIVE_ROLE_GRANTEES");
        defaultSystemViews.add("EFFECTIVE_ROLES");
        defaultSystemViews.add("EFFECTIVE_STRUCTURED_PRIVILEGES");
        defaultSystemViews.add("ELEMENT_TYPES");
        defaultSystemViews.add("ENCRYPTION_ROOT_KEYS");
        defaultSystemViews.add("EPM_MODELS");
        defaultSystemViews.add("EPM_QUERY_SOURCES");
        defaultSystemViews.add("EXPLAIN_PLAN_TABLE");
        defaultSystemViews.add("FLEXIBLE_TABLES");
        defaultSystemViews.add("FULL_SYSTEM_INFO_DUMPS");
        defaultSystemViews.add("FULLTEXT_INDEXES");
        defaultSystemViews.add("FUNCTION_PARAMETER_COLUMNS");
        defaultSystemViews.add("FUNCTION_PARAMETERS");
        defaultSystemViews.add("FUNCTIONS");
        defaultSystemViews.add("GEOCODE_INDEXES");
        defaultSystemViews.add("GRANTED_PRIVILEGES");
        defaultSystemViews.add("GRANTED_ROLES");
        defaultSystemViews.add("GRAPH_WORKSPACES");
        defaultSystemViews.add("HIERARCHY_OBJECTS");
        defaultSystemViews.add("HINTS");
        defaultSystemViews.add("INDEXES");
        defaultSystemViews.add("INDEX_COLUMNS");
        defaultSystemViews.add("INVALID_CONNECT_ATTEMPTS");
        defaultSystemViews.add("JWT_PROVIDERS");
        defaultSystemViews.add("JWT_USER_MAPPINGS");
        defaultSystemViews.add("LCM_PRODUCT_INSTANCES");
        defaultSystemViews.add("LCM_PRODUCT_INSTANCES_INCLUDED");
        defaultSystemViews.add("LCM_PRODUCTS");
        defaultSystemViews.add("LCM_SOFTWARE_COMPONENTS");
        defaultSystemViews.add("LCM_SWID");
        defaultSystemViews.add("LDAP_PROVIDER_URLS");
        defaultSystemViews.add("LDAP_PROVIDERS");
        defaultSystemViews.add("LDAP_USERS");
        defaultSystemViews.add("LIBRARIES");
        defaultSystemViews.add("LIBRARY_MEMBERS");
        defaultSystemViews.add("M_ACTIVE_PROCEDURES");
        defaultSystemViews.add("M_ACTIVE_STATEMENTS");
        defaultSystemViews.add("M_ADMISSION_CONTROL_EVENTS");
        defaultSystemViews.add("M_ADMISSION_CONTROL_QUEUES");
        defaultSystemViews.add("M_ADMISSION_CONTROL_STATISTICS");
        defaultSystemViews.add("M_AFL_FUNCTIONS");
        defaultSystemViews.add("M_AFL_STATES");
        defaultSystemViews.add("M_ATTACHED_STORAGES");
        defaultSystemViews.add("M_BACKUP_CATALOG");
        defaultSystemViews.add("M_BACKUP_CATALOG_FILES");
        defaultSystemViews.add("M_BACKUP_CONFIGURATION");
        defaultSystemViews.add("M_BACKUP_PROGRESS");
        defaultSystemViews.add("M_BACKUP_SIZE_ESTIMATIONS");
        defaultSystemViews.add("M_BLOCKED_TRANSACTIONS");
        defaultSystemViews.add("M_CACHE_ENTRIES");
        defaultSystemViews.add("M_CACHES");
        defaultSystemViews.add("M_CACHES_RESET");
        defaultSystemViews.add("M_CATALOG_MEMORY");
        defaultSystemViews.add("M_CE_CALCSCENARIO_HINTS");
        defaultSystemViews.add("M_CE_CALCSCENARIOS");
        defaultSystemViews.add("M_CE_CALCSCENARIOS_OVERVIEW");
        defaultSystemViews.add("M_CE_CALCVIEW_DEPENDENCIES");
        defaultSystemViews.add("M_CE_DEBUG_INFOS");
        defaultSystemViews.add("M_CE_DEBUG_JSONS");
        defaultSystemViews.add("M_CE_DEBUG_NODE_MAPPING");
        defaultSystemViews.add("M_CE_PLE_CALCSCENARIOS");
        defaultSystemViews.add("M_CLIENT_VERSIONS");
        defaultSystemViews.add("M_COLLECTION_TABLE_VIRTUAL_FILES");
        defaultSystemViews.add("M_COLLECTIONS_TABLES");
        defaultSystemViews.add("M_COMPACTION_THREAD");
        defaultSystemViews.add("M_CONDITIONAL_VARIABLES");
        defaultSystemViews.add("M_CONDITIONAL_VARIABLES_RESET");
        defaultSystemViews.add("M_CONNECTION_STATISTICS");
        defaultSystemViews.add("M_CONNECTIONS");
        defaultSystemViews.add("M_CONTAINER_DIRECTORY");
        defaultSystemViews.add("M_CONTAINER_NAME_DIRECTORY");
        defaultSystemViews.add("M_CONTEXT_MEMORY");
        defaultSystemViews.add("M_CONTEXT_MEMORY_RESET");
        defaultSystemViews.add("M_CONVERTER_STATISTICS");
        defaultSystemViews.add("M_CONVERTER_STATISTICS_RESET");
        defaultSystemViews.add("M_CS_ALL_COLUMN_STATISTICS");
        defaultSystemViews.add("M_CS_ALL_COLUMNS");
        defaultSystemViews.add("M_CS_COLUMNS");
        defaultSystemViews.add("M_CS_COLUMNS_PERSISTENCE");
        defaultSystemViews.add("M_CS_INDEXES");
        defaultSystemViews.add("M_CS_LOADS");
        defaultSystemViews.add("M_CS_LOB_SPACE_RECLAIMS");
        defaultSystemViews.add("M_CS_LOG_REPLAY_QUEUE_STATISTICS");
        defaultSystemViews.add("M_CS_LOG_REPLAY_QUEUE_STATISTICS_RESET");
        defaultSystemViews.add("M_CS_MVCC");
        defaultSystemViews.add("M_CS_PARTITIONS");
        defaultSystemViews.add("M_CS_TABLES");
        defaultSystemViews.add("M_CS_UNLOADS");
        defaultSystemViews.add("M_CUSTOMIZABLE_FUNCTIONALITIES");
        defaultSystemViews.add("M_DATA_STATISTICS");
        defaultSystemViews.add("M_DATABASE");
        defaultSystemViews.add("M_DATABASES");
        defaultSystemViews.add("M_DATABASE_HISTORY");
        defaultSystemViews.add("M_DATABASE_REPLICAS");
        defaultSystemViews.add("M_DATABASE_REPLICA_STATISTICS");
        defaultSystemViews.add("M_DATA_VOLUME_PAGE_STATISTICS");
        defaultSystemViews.add("M_DATA_VOLUME_PAGE_STATISTICS_RESET");
        defaultSystemViews.add("M_DATA_VOLUME_PARTITION_STATISTICS");
        defaultSystemViews.add("M_DATA_VOLUME_STATISTICS");
        defaultSystemViews.add("M_DATA_VOLUME_SUPERBLOCK_STATISTICS");
        defaultSystemViews.add("M_DATA_VOLUMES");
        defaultSystemViews.add("M_DEBUG_CONNECTIONS");
        defaultSystemViews.add("M_DEBUG_SESSIONS");
        defaultSystemViews.add("M_DELTA_MERGE_STATISTICS");
        defaultSystemViews.add("M_DISK_USAGE");
        defaultSystemViews.add("M_DISKS");
        defaultSystemViews.add("M_DSO_OPERATIONS");
        defaultSystemViews.add("M_DYNAMIC_RESULT_CACHE");
        defaultSystemViews.add("M_DYNAMIC_RESULT_CACHE_EXCLUSIONS");
        defaultSystemViews.add("M_EFFECTIVE_PASSWORD_POLICY");
        defaultSystemViews.add("M_EFFECTIVE_TABLE_PLACEMENT");
        defaultSystemViews.add("M_ENCRYPTION_OVERVIEW");
        defaultSystemViews.add("M_EPM_SESSIONS");
        defaultSystemViews.add("M_ERROR_CODES");
        defaultSystemViews.add("M_EVENTS");
        defaultSystemViews.add("M_EXECUTED_STATEMENTS");
        defaultSystemViews.add("M_EXPENSIVE_STATEMENT_EXECUTION_LOCATION_STATISTICS");
        defaultSystemViews.add("M_EXPENSIVE_STATEMENTS");
        defaultSystemViews.add("M_EXPORT_BINARY_STATUS");
        defaultSystemViews.add("M_EXTRACTORS");
        defaultSystemViews.add("M_FEATURE_USAGE");
        defaultSystemViews.add("M_FEATURES");
        defaultSystemViews.add("M_FULLTEXT_QUEUES");
        defaultSystemViews.add("M_FUZZY_SEARCH_INDEXES");
        defaultSystemViews.add("M_GARBAGE_COLLECTION_STATISTICS");
        defaultSystemViews.add("M_GARBAGE_COLLECTION_STATISTICS_RESET");
        defaultSystemViews.add("M_HA_DR_PROVIDERS");
        defaultSystemViews.add("M_HEAP_MEMORY");
        defaultSystemViews.add("M_HEAP_MEMORY_RESET");
        defaultSystemViews.add("M_HISTORY_INDEX_LAST_COMMIT_ID");
        defaultSystemViews.add("M_HOST_AGENT_INFORMATION");
        defaultSystemViews.add("M_HOST_AGENT_METRICS");
        defaultSystemViews.add("M_HOST_INFORMATION");
        defaultSystemViews.add("M_HOST_NETWORK_STATISTICS");
        defaultSystemViews.add("M_HOST_RESOURCE_UTILIZATION");
        defaultSystemViews.add("M_IMPORT_BINARY_STATUS");
        defaultSystemViews.add("M_INDEXING_QUEUES");
        defaultSystemViews.add("M_INIFILE_CONTENTS");
        defaultSystemViews.add("M_INIFILE_CONTENT_HISTORY");
        defaultSystemViews.add("M_INIFILES");
        defaultSystemViews.add("M_JOBEXECUTORS");
        defaultSystemViews.add("M_JOBEXECUTORS_RESET");
        defaultSystemViews.add("M_JOB_PROGRESS");
        defaultSystemViews.add("M_JOB_HISTORY_INFO");
        defaultSystemViews.add("M_JOIN_DATA_STATISTICS");
        defaultSystemViews.add("M_JOIN_TRANSLATION_TABLES");
        defaultSystemViews.add("M_JOINENGINE_STATISTICS");
        defaultSystemViews.add("M_KERNEL_PROFILER");
        defaultSystemViews.add("M_LANDSCAPE_HOST_CONFIGURATION");
        defaultSystemViews.add("M_LICENSE");
        defaultSystemViews.add("M_LICENSE_MEASUREMENTS");
        defaultSystemViews.add("M_LICENSE_MEASUREMENT_STATISTICS");
        defaultSystemViews.add("M_LICENSE_USAGE_HISTORY");
        defaultSystemViews.add("M_LICENSES");
        defaultSystemViews.add("M_LIVECACHE_CONTAINER_STATISTICS");
        defaultSystemViews.add("M_LIVECACHE_CONTAINER_STATISTICS_RESET");
        defaultSystemViews.add("M_LIVECACHE_LOCK_STATISTICS");
        defaultSystemViews.add("M_LIVECACHE_LOCK_STATISTICS_RESET");
        defaultSystemViews.add("M_LIVECACHE_LOCKS");
        defaultSystemViews.add("M_LIVECACHE_OMS_VERSIONS");
        defaultSystemViews.add("M_LIVECACHE_PROCEDURE_STATISTICS");
        defaultSystemViews.add("M_LIVECACHE_PROCEDURE_STATISTICS_RESET");
        defaultSystemViews.add("M_LIVECACHE_SCHEMA_STATISTICS");
        defaultSystemViews.add("M_LIVECACHE_SCHEMA_STATISTICS_RESET");
        defaultSystemViews.add("M_LOAD_HISTORY_HOST");
        defaultSystemViews.add("M_LOAD_HISTORY_INFO");
        defaultSystemViews.add("M_LOAD_HISTORY_SERVICE");
        defaultSystemViews.add("M_LOCK_WAITS_STATISTICS");
        defaultSystemViews.add("M_LOG_BUFFERS");
        defaultSystemViews.add("M_LOG_BUFFERS_RESET");
        defaultSystemViews.add("M_LOG_PARTITIONS");
        defaultSystemViews.add("M_LOG_PARTITIONS_RESET");
        defaultSystemViews.add("M_LOG_REPLAY_QUEUE_STATISTICS");
        defaultSystemViews.add("M_LOG_REPLAY_QUEUE_STATISTICS_RESET");
        defaultSystemViews.add("M_LOG_SEGMENTS");
        defaultSystemViews.add("M_LOG_SEGMENTS_RESET");
        defaultSystemViews.add("M_MEMORY_OBJECT_DISPOSITIONS");
        defaultSystemViews.add("M_MEMORY_OBJECTS");
        defaultSystemViews.add("M_MEMORY_OBJECTS_RESET");
        defaultSystemViews.add("M_MEMORY_RECLAIM_STATISTICS");
        defaultSystemViews.add("M_MEMORY_RECLAIM_STATISTICS_RESET");
        defaultSystemViews.add("M_MERGED_TRACES");
        defaultSystemViews.add("M_METADATA_CACHE_STATISTICS");
        defaultSystemViews.add("M_MONITOR_COLUMNS");
        defaultSystemViews.add("M_MONITORS");
        defaultSystemViews.add("M_MULTIDIMENSIONAL_STATEMENT_STATISTICS");
        defaultSystemViews.add("M_MUTEXES");
        defaultSystemViews.add("M_MUTEXES_RESET");
        defaultSystemViews.add("M_MVCC_OVERVIEW");
        defaultSystemViews.add("M_MVCC_SNAPSHOTS");
        defaultSystemViews.add("M_MVCC_TABLES");
        defaultSystemViews.add("M_NUMA_NODES");
        defaultSystemViews.add("M_NUMA_RESOURCES");
        defaultSystemViews.add("M_OBJECT_LOCK_STATISTICS");
        defaultSystemViews.add("M_OBJECT_LOCK_STATISTICS_RESET");
        defaultSystemViews.add("M_OBJECT_LOCKS");
        defaultSystemViews.add("M_OUT_OF_MEMORY_EVENTS");
        defaultSystemViews.add("M_PAGEACCESS_STATISTICS");
        defaultSystemViews.add("M_PAGEACCESS_STATISTICS_RESET");
        defaultSystemViews.add("M_PASSWORD_POLICY");
        defaultSystemViews.add("M_PERFTRACE");
        defaultSystemViews.add("M_PERSISTENCE_ENCRYPTION_KEYS");
        defaultSystemViews.add("M_PERSISTENCE_ENCRYPTION_STATUS");
        defaultSystemViews.add("M_PERSISTENCE_MANAGERS");
        defaultSystemViews.add("M_PERSISTENCE_MANAGERS_RESET");
        defaultSystemViews.add("M_PERSISTENT_MEMORY_VOLUMES");
        defaultSystemViews.add("M_PERSISTENT_MEMORY_VOLUME_DATA_FILES");
        defaultSystemViews.add("M_PERSISTENT_MEMORY_VOLUME_STATISTICS");
        defaultSystemViews.add("M_PLUGIN_MANIFESTS");
        defaultSystemViews.add("M_PLUGIN_STATUS");
        defaultSystemViews.add("M_PREPARED_STATEMENTS");
        defaultSystemViews.add("M_READWRITELOCKS");
        defaultSystemViews.add("M_READWRITELOCKS_RESET");
        defaultSystemViews.add("M_RECORD_LOCKS");
        defaultSystemViews.add("M_REMOTE_CONNECTIONS");
        defaultSystemViews.add("M_REMOTE_SOURCE_STATISTICS");
        defaultSystemViews.add("M_REMOTE_STATEMENTS");
        defaultSystemViews.add("M_REORG_ALGORITHMS");
        defaultSystemViews.add("M_REPO_TRANSPORT_FILES");
        defaultSystemViews.add("M_RESULT_CACHE");
        defaultSystemViews.add("M_RESULT_CACHE_RESET");
        defaultSystemViews.add("M_RESULT_CACHE_EXCLUSIONS");
        defaultSystemViews.add("M_RS_INDEXES");
        defaultSystemViews.add("M_RS_MEMORY");
        defaultSystemViews.add("M_RS_TABLE_VERSION_STATISTICS");
        defaultSystemViews.add("M_RS_TABLES");
        defaultSystemViews.add("M_SAVEPOINT_STATISTICS");
        defaultSystemViews.add("M_SAVEPOINT_STATISTICS_RESET");
        defaultSystemViews.add("M_SAVEPOINTS");
        defaultSystemViews.add("M_SEMAPHORES");
        defaultSystemViews.add("M_SEMAPHORES_RESET");
        defaultSystemViews.add("M_SEQUENCES");
        defaultSystemViews.add("M_SERIES_TABLES");
        defaultSystemViews.add("M_SERVICE_COMPONENT_MEMORY");
        defaultSystemViews.add("M_SERVICE_MEMORY");
        defaultSystemViews.add("M_SERVICE_NETWORK_IO");
        defaultSystemViews.add("M_SERVICE_NETWORK_IO_RESET");
        defaultSystemViews.add("M_SERVICE_NETWORK_METHOD_IO");
        defaultSystemViews.add("M_SERVICE_NETWORK_METHOD_IO_RESET");
        defaultSystemViews.add("M_SERVICE_REPLICATION");
        defaultSystemViews.add("M_SERVICE_STATISTICS");
        defaultSystemViews.add("M_SERVICE_THREADS");
        defaultSystemViews.add("M_SERVICE_THREAD_CALLSTACKS");
        defaultSystemViews.add("M_SERVICE_THREAD_SAMPLES");
        defaultSystemViews.add("M_SERVICE_TRACES");
        defaultSystemViews.add("M_SERVICE_TYPES");
        defaultSystemViews.add("M_SERVICES");
        defaultSystemViews.add("M_SESSION_CONTEXT");
        defaultSystemViews.add("M_SHARED_MEMORY");
        defaultSystemViews.add("M_SNAPSHOTS");
        defaultSystemViews.add("M_SQL_CLIENT_NETWORK_IO");
        defaultSystemViews.add("M_SQL_PLAN_CACHE");
        defaultSystemViews.add("M_SQL_PLAN_CACHE_RESET");
        defaultSystemViews.add("M_SQL_PLAN_CACHE_EXECUTION_LOCATION_STATISTICS");
        defaultSystemViews.add("M_SQL_PLAN_CACHE_EXECUTION_LOCATION_STATISTICS_RESET");
        defaultSystemViews.add("M_SQL_PLAN_CACHE_OVERVIEW");
        defaultSystemViews.add("M_SQL_PLAN_CACHE_PARAMETERS");
        defaultSystemViews.add("M_SQL_PLAN_STATISTICS");
        defaultSystemViews.add("M_SQL_PLAN_STATISTICS_RESET");
        defaultSystemViews.add("M_SQLSCRIPT_PLAN_PROFILER_RESULTS");
        defaultSystemViews.add("M_SQLSCRIPT_PLAN_PROFILERS");
        defaultSystemViews.add("M_STATISTICS_LASTVALUES");
        defaultSystemViews.add("M_SYSTEM_AVAILABILITY");
        defaultSystemViews.add("M_SYSTEM_INFORMATION_STATEMENTS");
        defaultSystemViews.add("M_SYSTEM_LIMITS");
        defaultSystemViews.add("M_SYSTEM_OVERVIEW");
        defaultSystemViews.add("M_SYSTEM_REPLICATION");
        defaultSystemViews.add("M_SYSTEM_REPLICATION_MVCC_HISTORY");
        defaultSystemViews.add("M_SYSTEM_REPLICATION_TAKEOVER_HISTORY");
        defaultSystemViews.add("M_TABLE_LOB_FILES");
        defaultSystemViews.add("M_TABLE_LOB_STATISTICS");
        defaultSystemViews.add("M_TABLE_LOCATIONS");
        defaultSystemViews.add("M_TABLE_LOCKS");
        defaultSystemViews.add("M_TABLE_PARTITION_STATISTICS");
        defaultSystemViews.add("M_TABLE_PARTITIONS");
        defaultSystemViews.add("M_TABLE_PERSISTENCE_LOCATIONS");
        defaultSystemViews.add("M_TABLE_PERSISTENCE_LOCATION_STATISTICS");
        defaultSystemViews.add("M_TABLE_PERSISTENCE_STATISTICS");
        defaultSystemViews.add("M_TABLE_PRUNING_STATISTICS");
        defaultSystemViews.add("M_TABLE_REPLICAS");
        defaultSystemViews.add("M_TABLE_REPLICAS_RESET");
        defaultSystemViews.add("M_TABLE_SNAPSHOTS");
        defaultSystemViews.add("M_TABLE_STATISTICS");
        defaultSystemViews.add("M_TABLE_STATISTICS_RESET");
        defaultSystemViews.add("M_TABLE_VIRTUAL_FILES");
        defaultSystemViews.add("M_TABLES");
        defaultSystemViews.add("M_TEMPORARY_JOIN_CONDITIONS");
        defaultSystemViews.add("M_TEMPORARY_JOIN_CONSTRAINTS");
        defaultSystemViews.add("M_TEMPORARY_KEY_FIGURES");
        defaultSystemViews.add("M_TEMPORARY_OBJECT_DEPENDENCIES");
        defaultSystemViews.add("M_TEMPORARY_TABLES");
        defaultSystemViews.add("M_TEMPORARY_TABLE_COLUMNS");
        defaultSystemViews.add("M_TEMPORARY_VIEW_COLUMNS");
        defaultSystemViews.add("M_TEMPORARY_VIEWS");
        defaultSystemViews.add("M_TENANTS");
        defaultSystemViews.add("M_TEXT_ANALYSIS_LANGUAGES");
        defaultSystemViews.add("M_TEXT_ANALYSIS_MIME_TYPES");
        defaultSystemViews.add("M_TIMEZONE_ALERTS");
        defaultSystemViews.add("M_TOPOLOGY_TREE");
        defaultSystemViews.add("M_TRACEFILE_CONTENTS");
        defaultSystemViews.add("M_TRACEFILES");
        defaultSystemViews.add("M_TRACE_CONFIGURATION");
        defaultSystemViews.add("M_TRACE_CONFIGURATION_RESET");
        defaultSystemViews.add("M_TRANS_TOKENS");
        defaultSystemViews.add("M_TRANSACTIONS");
        defaultSystemViews.add("M_UNDO_CLEANUP_FILES");
        defaultSystemViews.add("M_VERSION_MEMORY");
        defaultSystemViews.add("M_VOLUME_FILES");
        defaultSystemViews.add("M_VOLUME_IO_DETAILED_STATISTICS");
        defaultSystemViews.add("M_VOLUME_IO_DETAILED_STATISTICS_RESET");
        defaultSystemViews.add("M_VOLUME_IO_PERFORMANCE_STATISTICS");
        defaultSystemViews.add("M_VOLUME_IO_PERFORMANCE_STATISTICS_RESET");
        defaultSystemViews.add("M_VOLUME_IO_RETRY_STATISTICS");
        defaultSystemViews.add("M_VOLUME_IO_RETRY_STATISTICS_RESET");
        defaultSystemViews.add("M_VOLUME_IO_STATISTICS");
        defaultSystemViews.add("M_VOLUME_IO_STATISTICS_RESET");
        defaultSystemViews.add("M_VOLUME_IO_TOTAL_STATISTICS");
        defaultSystemViews.add("M_VOLUME_IO_TOTAL_STATISTICS_RESET");
        defaultSystemViews.add("M_VOLUME_SIZES");
        defaultSystemViews.add("M_VOLUMES");
        defaultSystemViews.add("M_WORKLOAD");
        defaultSystemViews.add("M_WORKLOAD_CAPTURES");
        defaultSystemViews.add("M_WORKLOAD_REPLAYS");
        defaultSystemViews.add("M_WORKLOAD_REPLAY_PREPROCESSES");
        defaultSystemViews.add("M_XB_MESSAGING_CONNECTIONS");
        defaultSystemViews.add("M_XB_MESSAGING_SUBSCRIPTIONS");
        defaultSystemViews.add("M_XS_APPLICATION_ISSUES");
        defaultSystemViews.add("M_XS_APPLICATIONS");
        defaultSystemViews.add("M_XS_PUBLIC_URLS");
        defaultSystemViews.add("M_XS_SESSIONS");
        defaultSystemViews.add("OBJECTS");
        defaultSystemViews.add("OBJECT_DEPENDENCIES");
        defaultSystemViews.add("OBJECT_PRIVILEGES");
        defaultSystemViews.add("OWNERSHIP");
        defaultSystemViews.add("PARTITIONED_TABLES");
        defaultSystemViews.add("PINNED_SQL_PLANS");
        defaultSystemViews.add("PRIVILEGES");
        defaultSystemViews.add("PROCEDURE_OBJECTS");
        defaultSystemViews.add("PROCEDURE_PARAMETERS");
        defaultSystemViews.add("PROCEDURE_PARAMETER_COLUMNS");
        defaultSystemViews.add("PROCEDURE_ROUTES");
        defaultSystemViews.add("PROJECTION_VIEW_COLUMN_SOURCES");
        defaultSystemViews.add("PROCEDURES");
        defaultSystemViews.add("PSE_CERTIFICATES");
        defaultSystemViews.add("PSES");
        defaultSystemViews.add("QUERY_PLANS");
        defaultSystemViews.add("REFERENTIAL_CONSTRAINTS");
        defaultSystemViews.add("REMOTE_SOURCES");
        defaultSystemViews.add("REMOTE_SUBSCRIPTION_DATA_CONTAINERS");
        defaultSystemViews.add("REMOTE_USERS");
        defaultSystemViews.add("REORG_OVERVIEW");
        defaultSystemViews.add("REORG_PLAN");
        defaultSystemViews.add("REORG_PLAN_INFOS");
        defaultSystemViews.add("REORG_STEPS");
        defaultSystemViews.add("RESERVED_KEYWORDS");
        defaultSystemViews.add("RESULT_CACHE");
        defaultSystemViews.add("RESULT_CACHE_COLUMNS");
        defaultSystemViews.add("ROLE_LDAP_GROUPS");
        defaultSystemViews.add("ROLES");
        defaultSystemViews.add("SAML_PROVIDER");
        defaultSystemViews.add("SAML_USER_MAPPINGS");
        defaultSystemViews.add("SCHEMAS");
        defaultSystemViews.add("SEARCH_RULE_SET_CONDITIONS");
        defaultSystemViews.add("SEARCH_RULE_SETS");
        defaultSystemViews.add("SEQUENCES");
        defaultSystemViews.add("SERIES_KEY_COLUMNS");
        defaultSystemViews.add("SERIES_TABLES");
        defaultSystemViews.add("SESSION_COOKIES");
        defaultSystemViews.add("SQLSCRIPT_TRACE");
        defaultSystemViews.add("STATEMENT_HINTS");
        defaultSystemViews.add("STRUCTURED_PRIVILEGES");
        defaultSystemViews.add("ST_GEOMETRY_COLUMNS");
        defaultSystemViews.add("ST_SPATIAL_REFERENCE_SYSTEMS");
        defaultSystemViews.add("ST_UNITS_OF_MEASURE");
        defaultSystemViews.add("SYNONYMS");
        defaultSystemViews.add("TABLE_COLUMNS");
        defaultSystemViews.add("TABLE_COLUMNS_ODBC");
        defaultSystemViews.add("TABLE_GROUPS");
        defaultSystemViews.add("TABLE_PARTITIONS");
        defaultSystemViews.add("TABLE_PLACEMENT");
        defaultSystemViews.add("TABLE_REPLICAS");
        defaultSystemViews.add("TABLES");
        defaultSystemViews.add("TEMPORAL_TABLES");
        defaultSystemViews.add("TEXT_CONFIGURATIONS");
        defaultSystemViews.add("TIMEZONES");
        defaultSystemViews.add("TRANSACTION_HISTORY");
        defaultSystemViews.add("TRIGGER_ORDERS");
        defaultSystemViews.add("TRIGGERS");
        defaultSystemViews.add("USER_PARAMETERS");
        defaultSystemViews.add("USERGROUPS");
        defaultSystemViews.add("USERGROUP_PARAMETERS");
        defaultSystemViews.add("USERS");
        defaultSystemViews.add("VIEW_PARAMETERS");
        defaultSystemViews.add("VIEWS");
        defaultSystemViews.add("VIEW_COLUMNS");
        defaultSystemViews.add("VIEW_EXPRESSION_MACROS");
        defaultSystemViews.add("VIRTUAL_COLUMN_PROPERTIES");
        defaultSystemViews.add("VIRTUAL_COLUMNS");
        defaultSystemViews.add("VIRTUAL_FUNCTION_PACKAGES");
        defaultSystemViews.add("VIRTUAL_FUNCTIONS");
        defaultSystemViews.add("VIRTUAL_TABLE_PARAMETERS");
        defaultSystemViews.add("VIRTUAL_TABLE_PROPERTIES");
        defaultSystemViews.add("VIRTUAL_TABLES");
        defaultSystemViews.add("VIRTUAL_PACKAGES");
        defaultSystemViews.add("VIRTUAL_PROCEDURES");
        defaultSystemViews.add("WORKLOAD_CLASSES");
        defaultSystemViews.add("WORKLOAD_MAPPINGS");
        defaultSystemViews.add("X509_USER_MAPPINGS");
        defaultSystemViews.add("XSA_AUDIT_LOG");
        return defaultSystemViews;
    }
}
