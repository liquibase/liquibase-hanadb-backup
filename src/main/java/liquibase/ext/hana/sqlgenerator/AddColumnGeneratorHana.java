package liquibase.ext.hana.sqlgenerator;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.hana.HanaDatabase;
import liquibase.sqlgenerator.core.AddColumnGenerator;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.core.AddColumnStatement;
import liquibase.util.StringUtil;

public class AddColumnGeneratorHana extends AddColumnGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(AddColumnStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    protected String generateSingleColumnSQL(AddColumnStatement statement, Database database) {
        DatabaseDataType columnType = DataTypeFactory.getInstance()
                .fromDescription(
                        statement.getColumnType() + (statement.isAutoIncrement() ? "{autoIncrement:true}" : ""),
                        database)
                .toDatabaseDataType(database);

        String alterTable = " ADD (" + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                statement.getTableName(), statement.getColumnName()) + " " + columnType;

        if (statement.isAutoIncrement()) {
            AutoIncrementConstraint autoIncrementConstraint = statement.getAutoIncrementConstraint();
            alterTable += " " + database.getAutoIncrementClause(autoIncrementConstraint.getStartWith(),
                    autoIncrementConstraint.getIncrementBy());
        }

        Object defaultValue = statement.getDefaultValue();
        if (defaultValue != null) {
            if (defaultValue instanceof DatabaseFunction) {
                alterTable += " DEFAULT " + DataTypeFactory.getInstance().fromObject(defaultValue, database)
                        .objectToSql(defaultValue, database);
            } else {
                alterTable += " DEFAULT " + DataTypeFactory.getInstance()
                        .fromDescription(statement.getColumnType(), database).objectToSql(defaultValue, database);
            }
        }

        if (!statement.isNullable()) {
            alterTable += " NOT NULL";
        }

        if (statement.isPrimaryKey()) {
            alterTable += " PRIMARY KEY";
        }

        if (statement.getRemarks() != null) {
            alterTable += " COMMENT '"
                    + database.escapeStringForDatabase(StringUtil.trimToEmpty(statement.getRemarks())) + "' ";
        }
        
        alterTable += ")";

        return alterTable;
    }
}
