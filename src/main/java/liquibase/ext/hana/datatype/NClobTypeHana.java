package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.ClobType;
import liquibase.ext.hana.HanaDatabase;

@DataTypeInfo(name = "nclob", aliases = { "longnvarchar", "ntext", "text", "longtext", "bintext", "tinytext",
		"mediumtext", "java.sql.Types.LONGNVARCHAR",
		"java.sql.Types.NCLOB" }, minParameters = 0, maxParameters = 0, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class NClobTypeHana extends ClobType {

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
		return new DatabaseDataType("NCLOB");
	}
}
