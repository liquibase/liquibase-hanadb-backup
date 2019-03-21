package liquibase.ext.hana.sqlgenerator;

import java.util.ArrayList;
import java.util.List;

import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateViewGenerator;
import liquibase.statement.core.CreateViewStatement;
import liquibase.util.SqlParser;
import liquibase.util.StringClauses;

public class CreateViewGeneratorHana extends CreateViewGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(CreateViewStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(CreateViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {

        List<Sql> sql = new ArrayList<>();

        StringClauses viewDefinition = SqlParser.parse(statement.getSelectQuery(), true, true);

        if (!statement.isFullDefinition()) {
            viewDefinition
                    .prepend(" ").prepend("AS").prepend(" ").prepend(database.escapeViewName(statement.getCatalogName(),
                            statement.getSchemaName(), statement.getViewName()))
                    .prepend(" ").prepend("VIEW").prepend(" ").prepend("CREATE");
        }

        if (statement.isReplaceIfExists()) {
            sql.add(new UnparsedSql(
                    "DO\n" + "BEGIN\n" + "DECLARE EXIT HANDLER FOR SQL_ERROR_CODE 321 BEGIN END;\n" + "EXEC 'DROP VIEW "
                            + database.escapeStringForDatabase(database.escapeViewName(statement.getCatalogName(),
                                    statement.getSchemaName(), statement.getViewName()))
                            + "'; \n" + "END--;;;",
                    "--;;;", getAffectedView(statement)));

            if (viewDefinition.contains("replace")) {
                viewDefinition.replace("CREATE OR REPLACE", "CREATE");
            }
        }
        sql.add(new UnparsedSql(viewDefinition.toString(), getAffectedView(statement)));
        return sql.toArray(new Sql[sql.size()]);
    }
}
