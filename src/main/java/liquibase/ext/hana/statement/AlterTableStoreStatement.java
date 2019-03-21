package liquibase.ext.hana.statement;

import liquibase.statement.AbstractSqlStatement;

public class AlterTableStoreStatement extends AbstractSqlStatement {
    private String schemaName;
    private String tableName;
    private String tableStore;

    public AlterTableStoreStatement(String schemaName, String tableName, String tableStore) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableStore = tableStore;
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableStore() {
        return this.tableStore;
    }

    public void setTableStore(String tableStore) {
        this.tableStore = tableStore;
    }

}
