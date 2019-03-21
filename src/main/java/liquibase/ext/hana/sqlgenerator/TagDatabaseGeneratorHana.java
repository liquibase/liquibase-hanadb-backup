package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.TagDatabaseGenerator;
import liquibase.statement.core.TagDatabaseStatement;
import liquibase.structure.core.Column;

public class TagDatabaseGeneratorHana extends TagDatabaseGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(TagDatabaseStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(TagDatabaseStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        String tableNameEscaped = database.escapeTableName(database.getLiquibaseCatalogName(),
                database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName());
        String orderColumnNameEscaped = database.escapeObjectName("ORDEREXECUTED", Column.class);
        String dateColumnNameEscaped = database.escapeObjectName("DATEEXECUTED", Column.class);
        String tagEscaped = DataTypeFactory.getInstance().fromObject(statement.getTag(), database)
                .objectToSql(statement.getTag(), database);

        return new Sql[] { new UnparsedSql("UPDATE " + tableNameEscaped + " t SET t.TAG=" + tagEscaped + " FROM (SELECT "
                + dateColumnNameEscaped + ", " + orderColumnNameEscaped + " FROM " + tableNameEscaped + " t ORDER BY "
                + dateColumnNameEscaped + " DESC, " + orderColumnNameEscaped + " DESC LIMIT 1) sub, " + tableNameEscaped + " t WHERE t."
                + dateColumnNameEscaped + "=sub." + dateColumnNameEscaped + " AND t." + orderColumnNameEscaped + "=sub."
                + orderColumnNameEscaped) };
    }
}
