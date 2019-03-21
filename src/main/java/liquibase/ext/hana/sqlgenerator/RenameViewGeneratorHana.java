package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RenameViewGenerator;
import liquibase.statement.core.RenameViewStatement;

public class RenameViewGeneratorHana extends RenameViewGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(RenameViewStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public ValidationErrors validate(RenameViewStatement statement, Database database,
            SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        validationErrors.addError("SAP HANA doesn't support renaming views");
        return validationErrors;
    }

}
