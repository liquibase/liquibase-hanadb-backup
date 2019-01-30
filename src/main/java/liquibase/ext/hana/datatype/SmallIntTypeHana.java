package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.SmallIntType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "smallint", aliases = { "java.sql.Types.SMALLINT",
        "int2" }, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class SmallIntTypeHana extends SmallIntType {
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
        return new DatabaseDataType("SMALLINT");
    }
}
