package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddAutoIncrementGenerator;
import liquibase.statement.core.AddAutoIncrementStatement;

public class AddAutoIncrementGeneratorHana extends AddAutoIncrementGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddAutoIncrementStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public ValidationErrors validate(AddAutoIncrementStatement statement, Database database,
            SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addError("SAP HANA doesn't support modifying columns to identity columns");
        return validationErrors;
    }
}
