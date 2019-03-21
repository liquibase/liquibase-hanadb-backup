package liquibase.ext.hana.datatype;

import liquibase.database.Database;
import liquibase.datatype.core.UnknownType;
import liquibase.ext.hana.HanaDatabase;

/**
 * Container for a data type that is not covered by any implementation in
 * {@link liquibase.datatype.core}. Most often, this class is used when a
 * DBMS-specific data type is given of which Liquibase does not know anything
 * about yet.
 */
public class UnknownTypeHana extends UnknownType {

    public UnknownTypeHana() {
        super();
    }

    public UnknownTypeHana(String name) {
        super(name);
    }

    public UnknownTypeHana(String name, int minParameters, int maxParameters) {
        super(name, minParameters, maxParameters);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return database instanceof HanaDatabase;
    }

}
