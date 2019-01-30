package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.UUIDType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "uuid", aliases = { "uniqueidentifier",
        "java.util.UUID" }, minParameters = 0, maxParameters = 0, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class UUIDTypeHana extends UUIDType {
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
        return new DatabaseDataType("VARCHAR", 36);
    }

}
