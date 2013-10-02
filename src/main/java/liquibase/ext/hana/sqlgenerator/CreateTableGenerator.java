package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.statement.core.CreateTableStatement;

public class CreateTableGenerator extends liquibase.sqlgenerator.core.CreateTableGenerator {
    @Override
    public boolean supports(CreateTableStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }


}
