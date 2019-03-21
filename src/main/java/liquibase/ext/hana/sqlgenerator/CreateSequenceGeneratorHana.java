package liquibase.ext.hana.sqlgenerator;

import java.math.BigInteger;

import liquibase.database.Database;
import liquibase.database.core.Db2zDatabase;
import liquibase.database.core.HsqlDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.database.core.SybaseASADatabase;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateSequenceGenerator;
import liquibase.statement.core.CreateSequenceStatement;

public class CreateSequenceGeneratorHana extends CreateSequenceGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateSequenceStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(CreateSequenceStatement statement, Database database,
            SqlGeneratorChain sqlGeneratorChain) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("CREATE SEQUENCE ");
        buffer.append(database.escapeSequenceName(statement.getCatalogName(), statement.getSchemaName(),
                statement.getSequenceName()));
        if (statement.getStartValue() != null) {
            buffer.append(" START WITH ").append(statement.getStartValue());
        }
        if (statement.getIncrementBy() != null) {
            buffer.append(" INCREMENT BY ").append(statement.getIncrementBy());
        }
        if (statement.getMinValue() != null) {
            buffer.append(" MINVALUE ").append(statement.getMinValue());
        }
        if (statement.getMaxValue() != null) {
            buffer.append(" MAXVALUE ").append(statement.getMaxValue());
        }

        if (statement.getCacheSize() != null) {
            if (BigInteger.ZERO.equals(statement.getCacheSize())) {
                buffer.append(" NO CACHE ");
            } else {
                buffer.append(" CACHE ").append(statement.getCacheSize());
            }
        }

        if (statement.getCycle() != null) {
            if (statement.getCycle().booleanValue()) {
                buffer.append(" CYCLE");
            }
        }

        return new Sql[] { new UnparsedSql(buffer.toString(), getAffectedSequence(statement)) };
    }
}
