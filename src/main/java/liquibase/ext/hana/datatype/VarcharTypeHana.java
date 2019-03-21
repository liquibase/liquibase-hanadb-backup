package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.VarcharType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "varchar", aliases = { "java.sql.Types.VARCHAR", "java.lang.String", "varchar2",
        "character varying" }, minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class VarcharTypeHana extends VarcharType {

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
