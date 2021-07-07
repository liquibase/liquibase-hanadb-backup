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
import liquibase.sqlgenerator.core.ModifyDataTypeGenerator;
import liquibase.statement.core.ModifyDataTypeStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ModifyDataTypeGeneratorHana extends ModifyDataTypeGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(ModifyDataTypeStatement statement, Database database) {
        return database instanceof HanaDatabase;
    }

    @Override
    public Sql[] generateSql(ModifyDataTypeStatement statement, Database database,
            SqlGeneratorChain sqlGeneratorChain) {
        List<Sql> sqls = new ArrayList<>();

        String alterTableBase = "ALTER TABLE " + database.escapeTableName(statement.getCatalogName(),
                statement.getSchemaName(), statement.getTableName());

        alterTableBase += " ALTER (";

        alterTableBase += database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                statement.getTableName(), statement.getColumnName());

        DatabaseDataType newDataType = DataTypeFactory.getInstance()
                .fromDescription(statement.getNewDataType(), database).toDatabaseDataType(database);

        DatabaseDataType oldDataType = LiquibaseHanaUtil.getColumnDataType(statement.getCatalogName(),
                statement.getSchemaName(), statement.getTableName(), statement.getColumnName(), database);

        if (oldDataType != null) {
            if (!isConvertible(oldDataType.toString(), newDataType.toString(), database)) {
                // add an intermediate conversion if possible
                if (isNumberType(oldDataType.toString())) {
                    // numbers can always be converted to decimal
                    sqls.add(new UnparsedSql(alterTableBase + " decimal)", getAffectedTable(statement)));

                } else if (isStringType(oldDataType.toString())) {
                    // strings can always be converted to nclob
                    sqls.add(new UnparsedSql(alterTableBase + " nclob)", getAffectedTable(statement)));
                }
            }
        }

        sqls.add(new UnparsedSql(alterTableBase + " " + newDataType + ")", getAffectedTable(statement)));

        return sqls.toArray(new Sql[sqls.size()]);
    }

    /**
     * Check if a data type can be converted to another data type. Data types can't
     * be converted if the target type has a lower precision than the source type.
     *
     * @param sourceType The source data type
     * @param targetType the target data type
     * @param database   The database
     * @return {@code true} if the type can be converted, {@code false} otherwise
     */
    private boolean isConvertible(String sourceType, String targetType, Database database) {
        if (Objects.equals(sourceType, targetType)) {
            return true;
        }

        if (sourceType == null || targetType == null) {
            return true;
        }

        if ("bigint".equals(sourceType)) {
            if ("integer".equals(targetType) || "smallint".equals(targetType) || "tinyint".equals(targetType)) {
                return false;
            }
        } else if ("integer".equals(sourceType)) {
            if ("smallint".equals(targetType) || "tinyint".equals(targetType)) {
                return false;
            }
        } else if ("smallint".equals(sourceType)) {
            if ("tinyint".equals(targetType)) {
                return false;
            }
        } else if ("double".equals(sourceType)) {
            if ("real".equals(targetType)) {
                return false;
            }
        }

        LiquibaseDataType liquibaseSourceType = DataTypeFactory.getInstance().fromDescription(sourceType, database);

        if ("float".equals(liquibaseSourceType.getName())) {
            if ("real".equals(targetType)) {
                return false;
            }
        } else if ("varchar".equals(liquibaseSourceType.getName())
                || "nvarchar".equals(liquibaseSourceType.getName())) {
            LiquibaseDataType liquibaseTargetType = DataTypeFactory.getInstance().fromDescription(targetType, database);
            if ("varchar".equals(liquibaseTargetType.getName()) || "nvarchar".equals(liquibaseTargetType.getName())) {
                if (getCharTypeLength(liquibaseSourceType) > getCharTypeLength(liquibaseTargetType)) {
                    return false;
                }
            }
        } else if ("decimal".equals(liquibaseSourceType.getName())) {
            LiquibaseDataType liquibaseTargetType = DataTypeFactory.getInstance().fromDescription(targetType, database);
            if ("decimal".equals(liquibaseTargetType.getName())) {
                if (getDecimalTypePrecision(liquibaseSourceType) > getDecimalTypePrecision(liquibaseTargetType)
                        || getDecimalTypeScale(liquibaseSourceType) > getDecimalTypeScale(liquibaseTargetType)) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean isNumberType(String type) {
        return type != null && ("bigint".equals(type) || "integer".equals(type) || "smallint".equals(type)
                || "tinyint".equals(type) || type.startsWith("float") || "real".equals(type) || "double".equals(type)
                || type.startsWith("decimal"));
    }

    private boolean isStringType(String type) {
        return type != null && (type.startsWith("varchar") || type.startsWith("nvarchar") || "clob".equals(type)
                || "nclob".equals(type));
    }

    private int getCharTypeLength(LiquibaseDataType type) {
        if (type.getParameters().length < 1) {
            // default length is 1
            return 1;
        }

        Object parameter = type.getParameters()[0];
        if (parameter == null) {
            return 1;
        }
        return Integer.parseInt(parameter.toString());
    }

    private int getDecimalTypePrecision(LiquibaseDataType type) {
        if (type.getParameters().length < 1) {
            // default precision is 34
            return 34;
        }

        Object parameter = type.getParameters()[0];
        if (parameter == null) {
            return 34;
        }
        return Integer.parseInt(parameter.toString());
    }

    private int getDecimalTypeScale(LiquibaseDataType type) {
        if (type.getParameters().length < 2) {
            // default scale is 0
            return 0;
        }

        Object parameter = type.getParameters()[1];
        if (parameter == null) {
            return 0;
        }
        return Integer.parseInt(parameter.toString());
    }
}
