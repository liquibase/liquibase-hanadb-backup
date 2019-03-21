package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.DateTimeType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "datetime", minParameters = 0, maxParameters = 1, aliases = { "java.sql.Types.DATETIME",
        "java.util.Date", "smalldatetime", "datetime2" }, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class DateTimeTypeHana extends DateTimeType {

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
        return new DatabaseDataType("TIMESTAMP");
    }
}
