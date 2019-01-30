package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateTableGenerator;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.DatabaseObject;

public class CreateTableGeneratorHana extends CreateTableGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateTableStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public ValidationErrors validate(CreateTableStatement createTableStatement, Database database,
            SqlGeneratorChain sqlGeneratorChain) {
        ValidationErrors validationErrors = super.validate(createTableStatement, database, sqlGeneratorChain);
        if (createTableStatement.getTablespace() != null) {
            if (!"ROW".equalsIgnoreCase(createTableStatement.getTablespace())
                    && !"COLUMN".equalsIgnoreCase(createTableStatement.getTablespace())) {
                validationErrors.addError("The tablespace must be either 'ROW' or 'COLUMN'");
            }
        }
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        Sql[] sqls = super.generateSql(statement, database, sqlGeneratorChain);
        Sql[] processedSqls = new Sql[sqls.length];
        for (int i = 0; i < sqls.length; i++) {
            Sql sql = sqls[i];
            if (sql.toSql().startsWith("CREATE TABLE ") && statement.getTablespace() != null) {
                processedSqls[i] = new UnparsedSql(
                        "CREATE " + statement.getTablespace() + " TABLE " + sql.toSql().substring(13),
                        sql.getEndDelimiter(), sql.getAffectedDatabaseObjects().toArray(new DatabaseObject[0]));
            } else {
                processedSqls[i] = sql;
            }
        }
        return processedSqls;
    }
}
