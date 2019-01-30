package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogLockTableStatement;

public class CreateDatabaseChangeLogLockTableGeneratorHana extends CreateDatabaseChangeLogLockTableGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogLockTableStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    protected String getCharTypeName(Database database) {
        return "NVARCHAR";
    }

    @Override
    protected String getDateTimeTypeString(Database database) {
        return "TIMESTAMP";
    }
}
