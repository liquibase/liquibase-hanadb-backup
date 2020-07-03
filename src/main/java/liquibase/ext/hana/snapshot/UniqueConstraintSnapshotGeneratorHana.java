package liquibase.ext.hana.snapshot;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import liquibase.Scope;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.hana.HanaDatabase;
import liquibase.snapshot.CachedRow;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.UniqueConstraintSnapshotGenerator;
import liquibase.statement.core.RawSqlStatement;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.structure.core.UniqueConstraint;

public class UniqueConstraintSnapshotGeneratorHana extends UniqueConstraintSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (database instanceof HanaDatabase) {
            return PRIORITY_DATABASE;
        } else {
            return PRIORITY_NONE; // Other DB? Let the generic handler do it.
        }
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[] { UniqueConstraintSnapshotGenerator.class };
    }

    @Override
    protected List<CachedRow> listConstraints(Table table, DatabaseSnapshot snapshot, Schema schema)
            throws DatabaseException, SQLException {
        return new ResultSetExtractorHana(snapshot, schema.getCatalogName(), schema.getName(), table.getName())
                .fastFetch();
    }


    @Override
    protected List<Map<String, ?>> listColumns(UniqueConstraint example, Database database, DatabaseSnapshot snapshot)
            throws DatabaseException {
        Relation table = example.getRelation();
        Schema schema = table.getSchema();
        String name = example.getName();

        String schemaName = database.correctObjectName(schema.getName(), Schema.class);
        String constraintName = database.correctObjectName(name, UniqueConstraint.class);
        String tableName = database.correctObjectName(table.getName(), Table.class);
        String sql = "select CONSTRAINT_NAME, COLUMN_NAME " + "from " + database.getSystemSchema() + ".CONSTRAINTS "
                + "where IS_UNIQUE_KEY='TRUE' AND IS_PRIMARY_KEY='FALSE' ";
        if (schemaName != null) {
            sql += "and SCHEMA_NAME='" + schemaName + "' ";
        }
        if (tableName != null) {
            sql += "and TABLE_NAME='" + tableName + "' ";
        }
        if (constraintName != null) {
            sql += "and CONSTRAINT_NAME='" + constraintName + "'";
        }
        List<Map<String, ?>> rows = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database)
                .queryForList(new RawSqlStatement(sql));

        return rows;

    }
}
