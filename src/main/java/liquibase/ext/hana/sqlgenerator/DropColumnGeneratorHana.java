package liquibase.ext.hana.sqlgenerator;

import java.util.ArrayList;
import java.util.List;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DropColumnGenerator;
import liquibase.statement.core.DropColumnStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Table;

public class DropColumnGeneratorHana extends DropColumnGenerator {
	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public boolean supports(DropColumnStatement statement, Database database) {
		if (statement.isMultiple()) {
			String tableName = null;
			for (DropColumnStatement s : statement.getColumns()) {
				if (tableName != null && !database.correctObjectName(tableName, Table.class).equals(database.correctObjectName(s.getTableName(), Table.class))) {
					return false;
				}
				tableName = s.getTableName();
			}
		}
		return database instanceof HanaDatabase;
	}

	@Override
	public Sql[] generateSql(DropColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
		StringBuilder sb = new StringBuilder();
		sb.append("ALTER TABLE ");
		List<Column> affectedColumns = new ArrayList<>();
		if (statement.isMultiple()) {
			boolean firstColumn = true;
			for (DropColumnStatement s : statement.getColumns()) {
				if (firstColumn) {
					sb.append(database.escapeTableName(s.getCatalogName(), s.getSchemaName(), s.getTableName()));
					sb.append(" DROP (");
				} else {
					sb.append(",");
				}
				sb.append(database.escapeColumnName(s.getCatalogName(), s.getSchemaName(), s.getTableName(),
						s.getColumnName()));
				firstColumn = false;
				affectedColumns.add(getAffectedColumn(s));
			}
		} else {
			sb.append(database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(),
					statement.getTableName()));
			sb.append(" DROP (");
			sb.append(database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
					statement.getTableName(), statement.getColumnName()));
			affectedColumns.add(getAffectedColumn(statement));
		}
		sb.append(")");
		return new Sql[] {
				new UnparsedSql(sb.toString(), affectedColumns.toArray(new Column[affectedColumns.size()])) };
	}
}
