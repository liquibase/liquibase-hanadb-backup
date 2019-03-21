package liquibase.ext.hana.change;

import java.util.Arrays;
import java.util.stream.Collectors;

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.change.core.AddLookupTableChange;
import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

@DatabaseChange(name = "addLookupTable", description = "Creates a lookup table containing values stored in a column and creates a foreign key to the new table.", priority = ChangeMetaData.PRIORITY_DATABASE, appliesTo = "column")
public class AddLookupTableChangeHana extends AddLookupTableChange {
    @Override
    public boolean supports(Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public SqlStatement[] generateStatements(Database database) {
        SqlStatement[] statements = super.generateStatements(database);
        return Arrays.stream(statements).map(statement -> {
            if (statement instanceof RawSqlStatement) {
                return new RawSqlStatement(statement.toString()
                        .replaceAll("(CREATE\\s+TABLE\\s+.+)(\\s+AS\\s+SELECT\\s+)(.+)", "$1 AS (SELECT $3)"));
            }
            return statement;
        }).collect(Collectors.toList()).toArray(new SqlStatement[0]);
    }
}
