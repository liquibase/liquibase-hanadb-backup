package liquibase.ext.hana.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.datatype.DatabaseDataType;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogService;
import liquibase.logging.LogType;
import liquibase.structure.core.Catalog;
import liquibase.structure.core.Column;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;

public class LiquibaseHanaUtil {

    private static final ReadWriteLock COLUMN_DATA_TYPE_CACHE_LOCK = new ReentrantReadWriteLock();

    private static final Map<String, Map<String, Map<String, DatabaseDataType>>> COLUMN_DATA_TYPE_CACHE = new HashMap<>();

    public static DatabaseDataType getColumnDataType(String catalog, String schema, String table, String column,
            Database database) {
        String catalogName = catalog == null ? database.getDefaultCatalogName()
                : database.correctObjectName(catalog, Catalog.class);
        String schemaName = schema == null ? database.getDefaultSchemaName()
                : database.correctObjectName(schema, Schema.class);
        String tableName = database.correctObjectName(table, Table.class);
        String columnName = database.correctObjectName(column, Column.class);
        DatabaseConnection databaseConnection = database.getConnection();
        if (databaseConnection instanceof JdbcConnection) {
            JdbcConnection connection = (JdbcConnection) databaseConnection;
            try (ResultSet rs = connection.getMetaData().getColumns(catalogName, schemaName, tableName, columnName)) {
                if (rs.next()) {
                    String typeName = rs.getString(6);
                    String columnSize = rs.getString(7);
                    String decimalDigits = rs.getString(9);

                    DatabaseDataType dataType;
                    if (decimalDigits == null) {
                        dataType = new DatabaseDataType(typeName, columnSize);
                    } else {
                        dataType = new DatabaseDataType(typeName, columnSize, decimalDigits);
                    }
                    DatabaseDataType dataTypeFromCache = getColumnDataTypeFromCache(schemaName, tableName, columnName);
                    if (!equals(dataType, dataTypeFromCache)) {
                        COLUMN_DATA_TYPE_CACHE_LOCK.writeLock().lock();
                        try {
                            dataTypeFromCache = getColumnDataTypeFromCache(schemaName, tableName, columnName);
                            if (!equals(dataType, dataTypeFromCache)) {
                                addColumnDataTypeToCache(schemaName, tableName, columnName, dataType);
                            }
                        } finally {
                            COLUMN_DATA_TYPE_CACHE_LOCK.writeLock().unlock();
                        }
                    }
                }
            } catch (DatabaseException | SQLException e) {
                LogService.getLog(LiquibaseHanaUtil.class).info(LogType.LOG,
                        "Could not get column information for column \"" + columnName + "\" of table \"" + schemaName
                                + "\".\"" + tableName + "\"",
                        e);
            }
        }

        return getColumnDataTypeFromCache(schemaName, tableName, columnName);
    }

    public static String getTableStore(String schema, String table, Database database) {
        String schemaName = schema == null ? database.getDefaultSchemaName()
                : database.correctObjectName(schema, Schema.class);
        String tableName = database.correctObjectName(table, Table.class);

        DatabaseConnection databaseConnection = database.getConnection();
        if (databaseConnection instanceof JdbcConnection) {
            JdbcConnection connection = (JdbcConnection) databaseConnection;
            try (PreparedStatement ps = connection
                    .prepareStatement("SELECT TABLE_TYPE FROM SYS.TABLES WHERE SCHEMA_NAME=? AND TABLE_NAME=?")) {
                ps.setString(1, schemaName);
                ps.setString(2, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            } catch (DatabaseException | SQLException e) {
                LogService.getLog(LiquibaseHanaUtil.class).info(LogType.LOG,
                        "Could not get table type information for table \"" + schemaName + "\".\"" + tableName + "\"",
                        e);
            }
        }

        return null;
    }

    private static DatabaseDataType getColumnDataTypeFromCache(String schema, String table, String column) {
        COLUMN_DATA_TYPE_CACHE_LOCK.readLock().lock();
        try {
            Map<String, Map<String, DatabaseDataType>> schemaMap = COLUMN_DATA_TYPE_CACHE.get(schema);
            if (schemaMap == null) {
                return null;
            }
            Map<String, DatabaseDataType> tableMap = schemaMap.get(table);
            if (tableMap == null) {
                return null;
            }
            return tableMap.get(column);
        } finally {
            COLUMN_DATA_TYPE_CACHE_LOCK.readLock().unlock();
        }
    }

    private static void addColumnDataTypeToCache(String schema, String table, String column,
            DatabaseDataType columnDataType) {
        COLUMN_DATA_TYPE_CACHE_LOCK.writeLock().lock();
        try {
            Map<String, Map<String, DatabaseDataType>> schemaMap = COLUMN_DATA_TYPE_CACHE.get(schema);
            if (schemaMap == null) {
                schemaMap = new HashMap<>();
                COLUMN_DATA_TYPE_CACHE.put(schema, schemaMap);
            }
            Map<String, DatabaseDataType> tableMap = schemaMap.get(table);
            if (tableMap == null) {
                tableMap = new HashMap<>();
                schemaMap.put(table, tableMap);
            }
            tableMap.put(column, columnDataType);
        } finally {
            COLUMN_DATA_TYPE_CACHE_LOCK.writeLock().unlock();
        }
    }

    private static boolean equals(DatabaseDataType d1, DatabaseDataType d2) {
        return (d1 == null && d2 == null) || (d1 != null && d2 != null && Objects.equals(d1.toString(), d2.toString()));
    }
}
