package liquibase.ext.hana;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HanaDatabaseTest {

    @Test
    public void getShortName() {
        assertEquals("hana", new HanaDatabase().getShortName());
    }

    public void supportsInitiallyDeferrableColumns() {
        assertFalse(new HanaDatabase().supportsInitiallyDeferrableColumns());
    }

    public void getCurrentDateTimeFunction() {
        assertEquals("CURRENT_TIMESTAMP", new HanaDatabase().getCurrentDateTimeFunction());
    }
}
