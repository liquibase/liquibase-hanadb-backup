package liquibase.ext.hana.sqlgenerator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import liquibase.datatype.DataTypeFactory;
import liquibase.ext.hana.HanaDatabase;
import liquibase.ext.hana.datatype.IntTypeHana;
import liquibase.sql.Sql;
import liquibase.statement.core.DropDefaultValueStatement;

public class DropDefaultValueGeneratorHanaTest {
    DropDefaultValueGeneratorHana generator = new DropDefaultValueGeneratorHana();
    HanaDatabase database = new HanaDatabase();

    @Test
    public void testGenerateSqlDropDefaultValueStatementDatabaseSqlGeneratorChain() {
        final DropDefaultValueStatement statement = new DropDefaultValueStatement("", "", "TABLE", "COLUMN", "INTEGER");
        DataTypeFactory.getInstance().register(IntTypeHana.class);
        final Sql[] sql = generator.generateSql(statement, database, null);
        assertEquals(1, sql.length);
        assertEquals("ALTER TABLE TABLE ALTER (COLUMN INTEGER DEFAULT NULL)", sql[0].toSql());
    }
}
