package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.BlobType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "blob", aliases = { "longvarbinary", "java.sql.Types.LONGVARBINARY", "java.sql.Types.VARBINARY",
        "java.sql.Types.BINARY", "varbinary", "bit varying",
        "binary" }, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class VarbinaryTypeHana extends BlobType {

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
        Object[] parameters = getParameters();
        if (parameters.length > 0) {
            return new DatabaseDataType("VARBINARY", parameters[0]);
        }
        return new DatabaseDataType("VARBINARY");
    }
}
