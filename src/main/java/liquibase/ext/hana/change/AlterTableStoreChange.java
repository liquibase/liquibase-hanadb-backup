package liquibase.ext.hana.change;

import liquibase.change.AbstractChange;
import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.hana.HanaDatabase;
import liquibase.ext.hana.statement.AlterTableStoreStatement;
import liquibase.statement.SqlStatement;

@DatabaseChange(name = "alterTableStore", description = "Alter Table Store (Column/Row)", priority = ChangeMetaData.PRIORITY_DATABASE)
public class AlterTableStoreChange extends AbstractChange {

    private String schemaName;
    private String tableName;
    private String tableStore;

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

    @Override
    public boolean supports(Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = super.validate(database);
        validationErrors.checkRequiredField("tableName", this.tableName);
        validationErrors.checkRequiredField("tableStore", this.tableStore);
        if (!"ROW".equalsIgnoreCase(this.tableStore) && !"COLUMN".equals(this.tableStore)) {
            validationErrors.addError("The table store type must be \"ROW\" or \"COLUMN\"");
        }
        return validationErrors;
    }

    @Override
    public String getConfirmationMessage() {
        return "Table " + (this.schemaName == null ? "" : this.schemaName + ".") + this.tableName + " changed to "
                + this.tableStore + " store table";
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        return new SqlStatement[] { new AlterTableStoreStatement(this.schemaName, this.tableName, this.tableStore) };
    }

}
