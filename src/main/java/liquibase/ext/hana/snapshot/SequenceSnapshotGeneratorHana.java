package liquibase.ext.hana.snapshot;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.snapshot.SnapshotGenerator;
import liquibase.snapshot.jvm.SequenceSnapshotGenerator;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;

public class SequenceSnapshotGeneratorHana extends SequenceSnapshotGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        int priority = super.getPriority(objectType, database);
        if (database instanceof HanaDatabase) {
            priority += PRIORITY_DATABASE;
        }
        return priority;
    }

    @Override
    public Class<? extends SnapshotGenerator>[] replaces() {
        return new Class[] { SequenceSnapshotGenerator.class };
    }

    @Override
    protected String getSelectSequenceSql(Schema schema, Database database) {
        if (database instanceof HanaDatabase) {
            return "SELECT SEQUENCE_NAME FROM SYS.SEQUENCES WHERE SCHEMA_NAME='" + schema.getName() + "' AND LEFT(SEQUENCE_NAME, 5) != '_SYS_'";
        }

        return super.getSelectSequenceSql(schema, database);

    }

}
