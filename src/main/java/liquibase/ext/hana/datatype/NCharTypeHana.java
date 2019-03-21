package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.NCharType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "nchar", aliases = { "java.sql.Types.NCHAR",
        "nchar2" }, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class NCharTypeHana extends NCharType {

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
            return new DatabaseDataType("NVARCHAR", parameters[0]);
        }
        return new DatabaseDataType("NVARCHAR");
    }

}