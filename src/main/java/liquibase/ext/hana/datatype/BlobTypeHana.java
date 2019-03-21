package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.BlobType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "blob", aliases = { "longblob", "java.sql.Types.BLOB", "java.sql.Types.LONGBLOB", "image",
        "tinyblob",
        "mediumblob" }, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class BlobTypeHana extends BlobType {

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
        return new DatabaseDataType("BLOB");
    }
}
