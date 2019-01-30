package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RenameTableGenerator;
import liquibase.statement.core.RenameTableStatement;

public class RenameTableGeneratorHana extends RenameTableGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(RenameTableStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(RenameTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[] {
                new UnparsedSql(
                        "RENAME TABLE "
                                + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(),
                                        statement.getOldTableName())
                                + " TO "
                                + database.escapeTableName(statement.getCatalogName(), statement.getSchemaName(),
                                        statement.getNewTableName()),
                        getAffectedOldTable(statement), getAffectedNewTable(statement)) };
    }
}
