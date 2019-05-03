package liquibase.ext.hana.change;

import liquibase.change.Change;
import liquibase.change.ChangeFactory;
import liquibase.change.ChangeMetaData;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.ext.hana.statement.AlterTableStoreStatement;
import liquibase.ext.hana.testing.BaseTestCase;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AlterTableStoreChangeTest extends BaseTestCase {

    @Before
    public void setUp() throws Exception {
        changeLogFile = "changelogs/AlterTableStoreChange/changelog.test.xml";
        connectToDB();
        cleanDB();
    }

    @Test
    public void test() throws Exception {
        if (connection == null) {
            return;
        }
        liquiBase.update((String) null);
    }

    @Test
    public void generateStatement() {
        AlterTableStoreChange change = new AlterTableStoreChange();

        change.setTableName("MY_TAB");
        change.setTableStore("ROW");

        HanaDatabase database = new HanaDatabase();

        SqlStatement[] statements = change.generateStatements(database);
        assertEquals(1, statements.length);
        AlterTableStoreStatement statement = (AlterTableStoreStatement) statements[0];

        assertEquals(database.getDefaultSchemaName(), statement.getSchemaName());
        assertEquals("MY_TAB", statement.getTableName());
        assertEquals("ROW", statement.getTableStore());
    }

    @Test
    public void getConfirmationMessage() {
        AlterTableStoreChange change = new AlterTableStoreChange();

        change.setTableName("MY_TAB");
        change.setTableStore("ROW");

        assertEquals("Table " + change.getTableName() + " changed to " + change.getTableStore() + " store table",
                change.getConfirmationMessage());

        change.setSchemaName("MY_SCHEMA");

        assertEquals("Table " + change.getSchemaName() + "." + change.getTableName() + " changed to "
                + change.getTableStore() + " store table", change.getConfirmationMessage());
    }

    @Test
    public void getChangeMetaData() {
        AlterTableStoreChange mergeTablesChange = new AlterTableStoreChange();

        ChangeFactory changeFactory = ChangeFactory.getInstance();

        assertEquals("alterTableStore", changeFactory.getChangeMetaData(mergeTablesChange).getName());
        assertEquals("Alter Table Store (Column/Row)",
                changeFactory.getChangeMetaData(mergeTablesChange).getDescription());
        assertEquals(ChangeMetaData.PRIORITY_DATABASE,
                changeFactory.getChangeMetaData(mergeTablesChange).getPriority());
    }

    @Test
    public void parseAndGenerate() throws Exception {
        if (connection == null) {
            return;
        }

        Database database = liquiBase.getDatabase();
        ResourceAccessor resourceAccessor = new ClassLoaderResourceAccessor();

        ChangeLogParameters changeLogParameters = new ChangeLogParameters();

        DatabaseChangeLog changeLog = ChangeLogParserFactory.getInstance().getParser(changeLogFile, resourceAccessor)
                .parse(changeLogFile, changeLogParameters, resourceAccessor);

        changeLog.validate(database);

        List<ChangeSet> changeSets = changeLog.getChangeSets();

        List<String> expectedQuery = new ArrayList<>();

        expectedQuery.add("ALTER TABLE LIQUIBASE_TEST.MY_TAB ROW");
        expectedQuery.add("ALTER TABLE LIQUIBASE_TEST.MY_TAB COLUMN");

        int i = 0;
        for (ChangeSet changeSet : changeSets) {
            for (Change change : changeSet.getChanges()) {
                Sql[] sql = SqlGeneratorFactory.getInstance().generateSql(change.generateStatements(database)[0],
                        database);
                if (i == 2) {
                    assertEquals(expectedQuery.get(0), sql[0].toSql());
                }
                else if (i == 3) {
                    assertEquals(expectedQuery.get(1), sql[0].toSql());
                }
            }
            i++;
        }
    }
}
