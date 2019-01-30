package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.executor.ExecutorService;
import liquibase.executor.LoggingExecutor;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sqlgenerator.core.InsertOrUpdateGenerator;
import liquibase.statement.core.InsertOrUpdateStatement;

public class InsertOrUpdateGeneratorHana extends InsertOrUpdateGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(InsertOrUpdateStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    protected String getRecordCheck(InsertOrUpdateStatement insertOrUpdateStatement, Database database,
            String whereClause) {

        StringBuffer recordCheckSql = new StringBuffer();

        recordCheckSql.append("DO\n");
        recordCheckSql.append("BEGIN\n");
        recordCheckSql.append("\tDECLARE record_count INT = 0;\n");
        recordCheckSql
                .append("\tSELECT COUNT(*) INTO record_count FROM "
                        + database.escapeTableName(insertOrUpdateStatement.getCatalogName(),
                                insertOrUpdateStatement.getSchemaName(), insertOrUpdateStatement.getTableName())
                        + " WHERE ");

        recordCheckSql.append(whereClause);
        recordCheckSql.append(";\n");

        recordCheckSql.append("\tIF record_count = 0 THEN\n");

        return recordCheckSql.toString();
    }

    @Override
    protected String getElse(Database database) {
        return "\tELSEIF record_count = 1 THEN\n";
    }

    @Override
    protected String getPostUpdateStatements(Database database) {
        StringBuffer endStatements = new StringBuffer();
        endStatements.append("END IF;\n");
        endStatements.append("END;\n");

        return endStatements.toString();

    }
}
