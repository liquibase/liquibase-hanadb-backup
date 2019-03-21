package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.TimestampType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "timestamp", aliases = { "java.sql.Types.TIMESTAMP", "java.sql.Timestamp",
        "timestamptz" }, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class TimestampTypeHana extends TimestampType {

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
