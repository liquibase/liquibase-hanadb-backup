package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.StoredProcedureGenerator;
import liquibase.statement.StoredProcedureStatement;
import liquibase.structure.DatabaseObject;

public class StoredProcedureGeneratorHana extends StoredProcedureGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(StoredProcedureStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(StoredProcedureStatement statement, Database database,
            SqlGeneratorChain sqlGeneratorChain) {
        Sql[] sqls = super.generateSql(statement, database, sqlGeneratorChain);
        Sql[] processedSqls = new Sql[sqls.length];
        for (int i = 0; i < sqls.length; i++) {
            String sql = sqls[i].toSql();
            processedSqls[i] = new UnparsedSql(sql.replaceAll("^exec ", "call "), sqls[i].getEndDelimiter(),
                    sqls[i].getAffectedDatabaseObjects().toArray(new DatabaseObject[0]));
        }
        return processedSqls;
    }
}
