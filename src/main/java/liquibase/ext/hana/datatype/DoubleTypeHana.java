package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.DoubleType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "double", aliases = { "java.sql.Types.DOUBLE",
        "java.lang.Double" }, minParameters = 0, maxParameters = 2, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class DoubleTypeHana extends DoubleType {
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
        return new DatabaseDataType("DOUBLE");
    }
}
