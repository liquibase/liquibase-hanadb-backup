package liquibase.ext.hana.sqlgenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

import liquibase.datatype.DataTypeFactory;
import liquibase.ext.hana.HanaDatabase;
import liquibase.ext.hana.datatype.IntTypeHana;
import liquibase.sql.Sql;
import liquibase.statement.core.DropColumnStatement;

public class DropColumnGeneratorHanaTest {
	DropColumnGeneratorHana generator = new DropColumnGeneratorHana();
	HanaDatabase database = new HanaDatabase();

	@Test
	public void testDropColumn() {
		final DropColumnStatement statement = new DropColumnStatement("", "", "TABLE", "COLUMN1");
		DataTypeFactory.getInstance().register(IntTypeHana.class);
		assertTrue(generator.supports(statement, database));
		final Sql[] sql = generator.generateSql(statement, database, null);
		assertEquals(1, sql.length);
		assertEquals("ALTER TABLE TABLE DROP (COLUMN1)", sql[0].toSql());
	}

	@Test
	public void testDropMultipleColumns() {
		final DropColumnStatement statement1 = new DropColumnStatement("", "", "TABLE", "COLUMN1");
		final DropColumnStatement statement2 = new DropColumnStatement("", "", "TABLE", "COLUMN2");
		final DropColumnStatement statement = new DropColumnStatement(
				Arrays.asList(new DropColumnStatement[] { statement1, statement2 }));
		DataTypeFactory.getInstance().register(IntTypeHana.class);
		assertTrue(generator.supports(statement, database));
		final Sql[] sql = generator.generateSql(statement, database, null);
		assertEquals(1, sql.length);
		assertEquals("ALTER TABLE TABLE DROP (COLUMN1,COLUMN2)", sql[0].toSql());
	}

	@Test
	public void testDropMultipleColumnsDifferentTables() {
		final DropColumnStatement statement1 = new DropColumnStatement("", "", "TABLE", "COLUMN1");
		final DropColumnStatement statement2 = new DropColumnStatement("", "", "TABLE2", "COLUMN2");
		final DropColumnStatement statement = new DropColumnStatement(
				Arrays.asList(new DropColumnStatement[] { statement1, statement2 }));
		DataTypeFactory.getInstance().register(IntTypeHana.class);
		assertFalse(generator.supports(statement, database));
	}
	
	@Test
	public void testDropMultipleColumnsSameTableDifferentCase() {
		final DropColumnStatement statement1 = new DropColumnStatement("", "", "TABLE", "COLUMN1");
		final DropColumnStatement statement2 = new DropColumnStatement("", "", "table", "COLUMN2");
		final DropColumnStatement statement = new DropColumnStatement(
				Arrays.asList(new DropColumnStatement[] { statement1, statement2 }));
		DataTypeFactory.getInstance().register(IntTypeHana.class);
		assertTrue(generator.supports(statement, database));
		final Sql[] sql = generator.generateSql(statement, database, null);
		assertEquals(1, sql.length);
		assertEquals("ALTER TABLE TABLE DROP (COLUMN1,COLUMN2)", sql[0].toSql());
	}
}
