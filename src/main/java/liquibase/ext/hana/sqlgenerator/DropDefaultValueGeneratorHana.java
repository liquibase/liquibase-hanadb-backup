package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.hana.HanaDatabase;
import liquibase.ext.hana.util.LiquibaseHanaUtil;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DropDefaultValueGenerator;
import liquibase.statement.core.DropDefaultValueStatement;

public class DropDefaultValueGeneratorHana extends DropDefaultValueGenerator {
	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public boolean supports(final DropDefaultValueStatement statement, final Database database) {
		return database instanceof HanaDatabase;
	}

	@Override
	public Sql[] generateSql(final DropDefaultValueStatement statement, final Database database,
			final SqlGeneratorChain sqlGeneratorChain) {
		LiquibaseDataType columnDataType = DataTypeFactory.getInstance().fromDescription(statement.getColumnDataType(),
				database);
		if (columnDataType == null) {
			columnDataType = DataTypeFactory.getInstance()
					.from(LiquibaseHanaUtil.getColumnDataType(statement.getCatalogName(), statement.getSchemaName(),
							statement.getTableName(), statement.getColumnName(), database), database);
		}
		return new Sql[] {
				new UnparsedSql(
						"ALTER TABLE "
								+ database.escapeTableName(
										statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
								+ " ALTER ("
								+ database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
										statement.getTableName(), statement.getColumnName())
								+ " " + columnDataType.toDatabaseDataType(database) + " DEFAULT NULL)",
						getAffectedColumn(statement)) };
	}
}
