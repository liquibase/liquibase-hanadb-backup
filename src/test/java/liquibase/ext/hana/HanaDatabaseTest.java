package liquibase.ext.hana;

import org.junit.Test;

import liquibase.database.AbstractJdbcDatabaseTest;

import static org.junit.Assert.*;

public class HanaDatabaseTest extends AbstractJdbcDatabaseTest {

    public HanaDatabaseTest() throws Exception {
        super(new HanaDatabase());
    }

    @Test
    public void getShortName() {
        assertEquals("hana", new HanaDatabase().getShortName());
    }

    @Override
    protected String getProductNameString() {
        return HanaDatabase.PRODUCT_NAME;
    }

    @Override
    public void supportsInitiallyDeferrableColumns() {
        assertFalse(getDatabase().supportsInitiallyDeferrableColumns());
    }

    @Override
    public void getCurrentDateTimeFunction() {
        assertEquals("CURRENT_TIMESTAMP", getDatabase().getCurrentDateTimeFunction());
    }
}