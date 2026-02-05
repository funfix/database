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
    @DisplayName("Sqlite driver should have correct class name")
    void testSqliteClassName() {
        JdbcDriver driver = JdbcDriver.Sqlite;
        assertEquals("org.sqlite.JDBC", driver.getClassName());
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
    @DisplayName("of() should find Sqlite driver by exact match")
    void testOfSqliteExactMatch() {
        JdbcDriver driver = JdbcDriver.invoke("org.sqlite.JDBC");
        assertNotNull(driver);
        assertSame(JdbcDriver.Sqlite, driver);
    }

    @Test
    @DisplayName("of() should find Sqlite driver by case-insensitive match")
    void testOfSqliteCaseInsensitive() {
        JdbcDriver driver = JdbcDriver.invoke("ORG.SQLITE.JDBC");
        assertNotNull(driver);
        assertSame(JdbcDriver.Sqlite, driver);
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
    @DisplayName("Sealed switch statement: exhaustive pattern matching on JdbcDriver")
    void testSealedSwitchStatement() {
        // Test that JdbcDriver works with exhaustive pattern matching in switch
        // This is the Java 17+ syntax for sealed classes
        var result = switchOnDriver(JdbcDriver.MsSqlServer);
        assertEquals("mssqlserver", result);

        result = switchOnDriver(JdbcDriver.Sqlite);
        assertEquals("sqlite", result);
    }

    /**
     * Demonstrates Java 21+ pattern matching with sealed interface.
     * This shows that JdbcDriver properly behaves as a sealed type on the Java side.
     */
    private String switchOnDriver(JdbcDriver driver) {
        return switch (driver) {
            case MsSqlServer -> "mssqlserver";
            case Sqlite -> "sqlite";
        };
    }

    @Test
    @DisplayName("All driver instances should be equal to themselves")
    void testDriverEquality() {
        //noinspection EqualsWithItself
        assertSame(JdbcDriver.MsSqlServer, JdbcDriver.MsSqlServer);
        //noinspection EqualsWithItself
        assertSame(JdbcDriver.Sqlite, JdbcDriver.Sqlite);
    }

    @Test
    @DisplayName("Different drivers should not be equal")
    void testDriverInequality() {
        assertNotEquals(JdbcDriver.MsSqlServer, JdbcDriver.Sqlite);
    }

    @Test
    @DisplayName("Drivers should have meaningful toString()")
    void testDriverToString() {
        String msSqlString = JdbcDriver.MsSqlServer.toString();
        assertTrue(msSqlString.contains("MsSqlServer"),
            "MsSqlServer toString should contain 'MsSqlServer': " + msSqlString);

        String sqliteString = JdbcDriver.Sqlite.toString();
        assertTrue(sqliteString.contains("Sqlite"),
            "Sqlite toString should contain 'Sqlite': " + sqliteString);
    }

    @Test
    @DisplayName("Switch expression on JdbcDriver without default branch")
    void testSwitchExpressionCoverage() {
        JdbcDriver driver = JdbcDriver.Sqlite;
        String result = switch (driver) {
            //noinspection DataFlowIssue
            case Sqlite -> "sqlite";
            case MsSqlServer -> "mssql";
        };
        assertEquals("sqlite", result);

        driver = JdbcDriver.MsSqlServer;
        result = switch (driver) {
            case Sqlite -> "sqlite";
            //noinspection DataFlowIssue
            case MsSqlServer -> "mssql";
        };
        assertEquals("mssql", result);
    }
}
