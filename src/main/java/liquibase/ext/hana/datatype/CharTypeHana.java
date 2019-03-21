package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.CharType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "char", aliases = { "java.sql.Types.CHAR",
        "bpchar" }, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class CharTypeHana extends CharType {
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
            return new DatabaseDataType("VARCHAR", parameters[0]);
        }
        return new DatabaseDataType("VARCHAR");
    }

}
