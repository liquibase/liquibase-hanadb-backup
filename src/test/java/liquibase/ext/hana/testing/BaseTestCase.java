package liquibase.ext.hana.testing;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.util.Properties;

import liquibase.Liquibase;
import liquibase.database.DatabaseConnection;
import liquibase.database.DatabaseFactory;
import liquibase.database.OfflineConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

public class BaseTestCase {
    private static String url;
    private static Driver driver;
    private static Properties info;
    protected static Connection connection;
    protected static DatabaseConnection jdbcConnection;
    protected static Liquibase liquiBase;
    protected static String changeLogFile;

    public static void connectToDB() throws Exception {
        if (connection == null) {
            info = new Properties();
            try (InputStream fis = new FileInputStream("src/test/resources/tests.properties")) {
                info.load(fis);
            }

            url = info.getProperty("url");
            try {
                driver = (Driver) Class.forName(DatabaseFactory.getInstance().findDefaultDriver(url), true,
                        Thread.currentThread().getContextClassLoader()).newInstance();

                connection = driver.connect(url, info);

                if (connection == null) {
                    throw new DatabaseException("Connection could not be created to " + url + " with driver "
                            + driver.getClass().getName() + ".  Possibly the wrong driver for the given database URL");
                }

                jdbcConnection = new JdbcConnection(connection);

            } catch (@SuppressWarnings("unused") ClassNotFoundException e) {
                jdbcConnection = new OfflineConnection("offline:sap?catalog=LIQUIBASE_TEST",
                        new ClassLoaderResourceAccessor());
            }

        }
    }

    public static void cleanDB() throws Exception {
        liquiBase = new Liquibase(changeLogFile, new ClassLoaderResourceAccessor(), jdbcConnection);
        if (connection != null) {
            liquiBase.dropAll();
        }
    }
}
