package liquibase.ext.hana;

import org.junit.Test;

import static org.junit.Assert.*;

public class HanaDatabaseTest {
    @Test
    public void getShortName() {
        assertEquals("hanadb", new HanaDatabase().getShortName());
    }
}