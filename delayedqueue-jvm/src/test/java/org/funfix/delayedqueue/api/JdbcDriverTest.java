package org.funfix.delayedqueue.api;

import static org.junit.jupiter.api.Assertions.*;

import org.funfix.delayedqueue.jvm.JdbcDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * Tests for JdbcDriver sealed interface.
 */
@DisplayName("JdbcDriver Tests")
class JdbcDriverTest {

    @Test
    @DisplayName("MsSqlServer driver should have correct class name")
    void testMsSqlServerClassName() {
        JdbcDriver driver = JdbcDriver.MsSqlServer;
        assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver", driver.getClassName());
    }

    @Test
    @DisplayName("HSQLDB driver should have correct class name")
    void testHsqlDbClassName() {
        JdbcDriver driver = JdbcDriver.HSQLDB;
        assertEquals("org.hsqldb.jdbc.JDBCDriver", driver.getClassName());
    }

    @Test
    @DisplayName("Oracle driver should have correct class name")
    void testOracleClassName() {
        JdbcDriver driver = JdbcDriver.Oracle;
        assertEquals("oracle.jdbc.OracleDriver", driver.getClassName());
    }

    @Test
    @DisplayName("of() should find MsSqlServer driver by exact match")
    void testOfMsSqlServerExactMatch() {
        JdbcDriver driver = JdbcDriver.invoke("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        assertNotNull(driver);
        assertSame(JdbcDriver.MsSqlServer, driver);
    }

    @Test
    @DisplayName("of() should find MsSqlServer driver by case-insensitive match")
    void testOfMsSqlServerCaseInsensitive() {
        JdbcDriver driver = JdbcDriver.invoke("COM.MICROSOFT.SQLSERVER.JDBC.SQLSERVERDRIVER");
        assertNotNull(driver);
        assertSame(JdbcDriver.MsSqlServer, driver);
    }

    @Test
    @DisplayName("of() should find HSQLDB driver by exact match")
    void testOfSqliteExactMatch() {
        JdbcDriver driver = JdbcDriver.invoke("org.hsqldb.jdbc.JDBCDriver");
        assertNotNull(driver);
        assertSame(JdbcDriver.HSQLDB, driver);
    }

    @Test
    @DisplayName("of() should find Oracle driver by exact match")
    void testOfOracleExactMatch() {
        JdbcDriver driver = JdbcDriver.invoke("oracle.jdbc.OracleDriver");
        assertNotNull(driver);
        assertSame(JdbcDriver.Oracle, driver);
    }

    @Test
    @DisplayName("of() should find HSQLDB driver by case-insensitive match")
    void testOfSqliteCaseInsensitive() {
        JdbcDriver driver = JdbcDriver.invoke("ORG.HSQLDB.JDBC.JDBCDRIVER");
        assertNotNull(driver);
        assertSame(JdbcDriver.HSQLDB, driver);
    }

    @Test
    @DisplayName("of() should return null for unknown driver")
    void testOfUnknownDriver() {
        JdbcDriver driver = JdbcDriver.invoke("com.unknown.jdbc.Driver");
        assertNull(driver);
    }

    @Test
    @DisplayName("of() should return null for empty string")
    void testOfEmptyString() {
        JdbcDriver driver = JdbcDriver.invoke("");
        assertNull(driver);
    }

    @Test
    @DisplayName("All driver instances should be accessible")
    void testDriverAccessibility() {
        // Test that all driver instances can be accessed
        assertNotNull(JdbcDriver.MsSqlServer);
        assertNotNull(JdbcDriver.HSQLDB);
        assertNotNull(JdbcDriver.H2);
        assertNotNull(JdbcDriver.Sqlite);
        assertNotNull(JdbcDriver.PostgreSQL);
        assertNotNull(JdbcDriver.MariaDB);
        assertNotNull(JdbcDriver.Oracle);
    }

    /**
     * Helper method to demonstrate driver differentiation using equals/identity.
     */
    private String getDriverName(JdbcDriver driver) {
        if (driver == JdbcDriver.HSQLDB) return "hsqldb";
        if (driver == JdbcDriver.H2) return "h2";
        if (driver == JdbcDriver.MsSqlServer) return "mssqlserver";
        if (driver == JdbcDriver.Sqlite) return "sqlite";
        if (driver == JdbcDriver.PostgreSQL) return "postgresql";
        if (driver == JdbcDriver.MariaDB) return "mariadb";
        if (driver == JdbcDriver.Oracle) return "oracle";
        throw new IllegalArgumentException("Unknown driver: " + driver);
    }

    @Test
    @DisplayName("Driver differentiation works correctly")
    void testDriverDifferentiation() {
        assertEquals("mssqlserver", getDriverName(JdbcDriver.MsSqlServer));
        assertEquals("hsqldb", getDriverName(JdbcDriver.HSQLDB));
        assertEquals("h2", getDriverName(JdbcDriver.H2));
        assertEquals("oracle", getDriverName(JdbcDriver.Oracle));
    }

    @Test
    @DisplayName("All driver instances should be equal to themselves")
    void testDriverEquality() {
        //noinspection EqualsWithItself
        assertSame(JdbcDriver.MsSqlServer, JdbcDriver.MsSqlServer);
        //noinspection EqualsWithItself
        assertSame(JdbcDriver.HSQLDB, JdbcDriver.HSQLDB);
        //noinspection EqualsWithItself
        assertSame(JdbcDriver.H2, JdbcDriver.H2);
        //noinspection EqualsWithItself
        assertSame(JdbcDriver.Oracle, JdbcDriver.Oracle);
    }

    @Test
    @DisplayName("Different drivers should not be equal")
    void testDriverInequality() {
        assertNotEquals(JdbcDriver.MsSqlServer, JdbcDriver.HSQLDB);
        assertNotEquals(JdbcDriver.H2, JdbcDriver.Oracle);
    }

    @Test
    @DisplayName("Drivers should have meaningful toString()")
    void testDriverToString() {
        String msSqlString = JdbcDriver.MsSqlServer.toString();
        assertTrue(msSqlString.contains("MsSqlServer"),
            "MsSqlServer toString should contain 'MsSqlServer': " + msSqlString);

        String sqliteString = JdbcDriver.HSQLDB.toString();
        assertTrue(sqliteString.contains("HSQLDB"),
            "Sqlite toString should contain 'HSQLDB': " + sqliteString);
    }

    @Test
    @DisplayName("Entries list contains all drivers")
    void testEntriesList() {
        var entries = JdbcDriver.getEntries();
        assertNotNull(entries);
        assertEquals(7, entries.size());
        assertTrue(entries.contains(JdbcDriver.HSQLDB));
        assertTrue(entries.contains(JdbcDriver.H2));
        assertTrue(entries.contains(JdbcDriver.MsSqlServer));
        assertTrue(entries.contains(JdbcDriver.Sqlite));
        assertTrue(entries.contains(JdbcDriver.PostgreSQL));
        assertTrue(entries.contains(JdbcDriver.MariaDB));
        assertTrue(entries.contains(JdbcDriver.Oracle));
    }
}
