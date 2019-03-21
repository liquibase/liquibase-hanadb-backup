package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;

public class CreateDatabaseChangeLogTableGeneratorHana extends CreateDatabaseChangeLogTableGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
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
