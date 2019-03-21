package liquibase.ext.hana.sqlgenerator;

import java.math.BigInteger;

import liquibase.database.Database;
import liquibase.database.core.FirebirdDatabase;
import liquibase.database.core.HsqlDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AlterSequenceGenerator;
import liquibase.statement.core.AlterSequenceStatement;

public class AlterSequenceGeneratorHana extends AlterSequenceGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AlterSequenceStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(AlterSequenceStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("ALTER SEQUENCE ");
        buffer.append(database.escapeSequenceName(statement.getCatalogName(), statement.getSchemaName(),
                statement.getSequenceName()));

        if (statement.getIncrementBy() != null) {
            buffer.append(" INCREMENT BY ").append(statement.getIncrementBy());
        }

        if (statement.getMinValue() != null) {
            buffer.append(" MINVALUE ").append(statement.getMinValue());
        }

        if (statement.getMaxValue() != null) {
            buffer.append(" MAXVALUE ").append(statement.getMaxValue());
        }

        if (statement.getCycle() != null) {
            if (statement.getCycle().booleanValue()) {
                buffer.append(" CYCLE ");
            } else {
                buffer.append(" NO CYCLE ");
            }
        }

        if (statement.getCacheSize() != null) {
            if (statement.getCacheSize().equals(BigInteger.ZERO)) {
                buffer.append(" NO CACHE ");
            } else {
                buffer.append(" CACHE ").append(statement.getCacheSize());
            }
        }

        return new Sql[] { new UnparsedSql(buffer.toString(), getAffectedSequence(statement)) };
    }
}
