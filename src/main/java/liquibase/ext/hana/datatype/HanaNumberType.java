package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.core.NumberType;
import liquibase.ext.hana.HanaDatabase;

public class HanaNumberType extends NumberType {

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
        return new DatabaseDataType("DECIMAL");
    }
}
