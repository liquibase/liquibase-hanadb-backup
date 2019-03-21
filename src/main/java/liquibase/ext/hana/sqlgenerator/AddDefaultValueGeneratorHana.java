package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.hana.HanaDatabase;
import liquibase.ext.hana.util.LiquibaseHanaUtil;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AddDefaultValueGenerator;
import liquibase.statement.core.AddDefaultValueStatement;

public class AddDefaultValueGeneratorHana extends AddDefaultValueGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddDefaultValueStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(AddDefaultValueStatement statement, Database database,
            SqlGeneratorChain sqlGeneratorChain) {
        Object defaultValue = statement.getDefaultValue();
        LiquibaseDataType columnDataType = DataTypeFactory.getInstance().fromDescription(statement.getColumnDataType(),
                database);
        if (columnDataType == null) {
            columnDataType = DataTypeFactory.getInstance()
                    .from(LiquibaseHanaUtil.getColumnDataType(statement.getCatalogName(), statement.getSchemaName(),
                            statement.getTableName(), statement.getColumnName(), database), database);
            if (columnDataType == null) {
                columnDataType = DataTypeFactory.getInstance().fromObject(defaultValue, database);
            }
        }
        return new Sql[] { new UnparsedSql("ALTER TABLE " + database
                .escapeTableName(statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                + " ALTER ("
                + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                        statement.getTableName(), statement.getColumnName())
                + " " + columnDataType.toDatabaseDataType(database) + " DEFAULT "
                + DataTypeFactory.getInstance().fromObject(defaultValue, database).objectToSql(defaultValue, database)
                + ")", getAffectedColumn(statement)) };
    }
}
