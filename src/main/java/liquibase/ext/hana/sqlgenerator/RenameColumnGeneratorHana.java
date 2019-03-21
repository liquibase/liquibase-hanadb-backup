package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RenameColumnGenerator;
import liquibase.statement.core.RenameColumnStatement;

public class RenameColumnGeneratorHana extends RenameColumnGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(RenameColumnStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(RenameColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[] {
                new UnparsedSql(
                        "RENAME COLUMN "
                                + database.escapeTableName(
                                        statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                                + "."
                                + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                                        statement.getTableName(), statement.getOldColumnName())
                                + " TO "
                                + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                                        statement.getTableName(), statement.getNewColumnName()),
                        getAffectedOldColumn(statement), getAffectedNewColumn(statement)) };
    }
}
