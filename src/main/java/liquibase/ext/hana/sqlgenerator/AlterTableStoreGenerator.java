package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.hana.HanaDatabase;
import liquibase.ext.hana.statement.AlterTableStoreStatement;
import liquibase.ext.hana.util.LiquibaseHanaUtil;
import liquibase.logging.LogService;
import liquibase.logging.LogType;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Table;

public class AlterTableStoreGenerator extends AbstractSqlGenerator<AlterTableStoreStatement> {

    @Override
    public ValidationErrors validate(AlterTableStoreStatement statement, Database database,
            SqlGeneratorChain<AlterTableStoreStatement> sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", statement.getTableName());
        validationErrors.checkRequiredField("tableStore", statement.getTableStore());
        return validationErrors;
    }

    @Override
    public boolean supports(AlterTableStoreStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(AlterTableStoreStatement statement, Database database,
            SqlGeneratorChain<AlterTableStoreStatement> sqlGeneratorChain) {
        String tableStore = LiquibaseHanaUtil.getTableStore(statement.getSchemaName(), statement.getTableName(),
                database);
        if (tableStore != null && tableStore.equalsIgnoreCase(statement.getTableStore())) {
            LogService.getLog(getClass()).info(LogType.LOG,
                    "The current store type of table " + statement.getSchemaName() + "." + statement.getTableName()
                            + " (" + tableStore + ") is equal to the requested store type (" + statement.getTableStore()
                            + ")");
            return new Sql[0];
        }

        return new Sql[] { new UnparsedSql(
                "ALTER TABLE " + database.escapeTableName(null, statement.getSchemaName(), statement.getTableName())
                        + " " + statement.getTableStore(),
                new Table(null, statement.getSchemaName(), statement.getTableName())) };
    }

}
