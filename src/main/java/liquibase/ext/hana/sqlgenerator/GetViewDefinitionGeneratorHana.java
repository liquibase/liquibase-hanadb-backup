package liquibase.ext.hana.sqlgenerator;

import liquibase.CatalogAndSchema;
import liquibase.database.Database;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.GetViewDefinitionGenerator;
import liquibase.statement.core.GetViewDefinitionStatement;
import liquibase.structure.core.View;

public class GetViewDefinitionGeneratorHana extends GetViewDefinitionGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(GetViewDefinitionStatement statement, Database database,
            SqlGeneratorChain sqlGeneratorChain) {
        CatalogAndSchema schema = new CatalogAndSchema(statement.getCatalogName(), statement.getSchemaName())
                .customize(database);

        return new Sql[] { new UnparsedSql("SELECT DEFINITION FROM SYS.VIEWS WHERE VIEW_NAME='"
                + database.correctObjectName(statement.getViewName(), View.class) + "' AND SCHEMA_NAME='"
                + schema.getSchemaName() + "'") };
    }

    @Override
    public boolean supports(GetViewDefinitionStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }
}
