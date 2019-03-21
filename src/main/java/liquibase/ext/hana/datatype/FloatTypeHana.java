package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.FloatType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "float", aliases = { "java.sql.Types.FLOAT", "java.lang.Float", "real",
        "java.sql.Types.REAL" }, minParameters = 0, maxParameters = 2, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class FloatTypeHana extends FloatType {

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
        Object[] parameters = this.getParameters();
        if (parameters.length > 0) {
            return new DatabaseDataType("FLOAT", parameters[0]);
        }
        return new DatabaseDataType("FLOAT");
    }

}
