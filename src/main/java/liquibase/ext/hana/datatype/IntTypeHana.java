package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.IntType;
import liquibase.ext.hana.HanaDatabase;

/**
 * Represents a signed integer number using 32 bits of storage.
 */
@DataTypeInfo(name = "int", aliases = { "integer", "java.sql.Types.INTEGER", "java.lang.Integer", "serial", "int4",
        "serial4" }, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class IntTypeHana extends IntType {

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
        return new DatabaseDataType("INTEGER");
    }
}
