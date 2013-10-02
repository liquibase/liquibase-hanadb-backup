package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.CurrencyType;
import liquibase.ext.hana.HanaDatabase;

public class HanaCurrencyType extends CurrencyType {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public DatabaseDataType toDatabaseDataType(Database database) {
        return new DatabaseDataType("DECIMAL(15,2)");
    }
}
