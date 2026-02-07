/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.funfix.delayedqueue.jvm.JdbcConnectionConfig;
import org.funfix.delayedqueue.jvm.JdbcDatabasePoolConfig;
import org.funfix.delayedqueue.jvm.JdbcDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * Tests for JdbcConnectionConfig record.
 */
@DisplayName("JdbcConnectionConfig Tests")
class JdbcConnectionConfigTest {

    @Test
    @DisplayName("Creating config with required parameters only")
    void testBasicConfig() {
        String url = "jdbc:sqlite:memory";
        JdbcDriver driver = JdbcDriver.HSQLDB;

        JdbcConnectionConfig config = new JdbcConnectionConfig(url, driver);

        assertEquals(url, config.url());
        assertEquals(driver, config.driver());
        assertNull(config.username());
        assertNull(config.password());
        assertNull(config.pool());
    }

    @Test
    @DisplayName("Creating config with all parameters")
    void testFullConfig() {
        String url = "jdbc:sqlite:memory";
        JdbcDriver driver = JdbcDriver.HSQLDB;
        String username = "testuser";
        String password = "testpass";
        JdbcDatabasePoolConfig poolConfig = new JdbcDatabasePoolConfig();

        JdbcConnectionConfig config = new JdbcConnectionConfig(
            url, driver, username, password, poolConfig
        );

        assertEquals(url, config.url());
        assertEquals(driver, config.driver());
        assertEquals(username, config.username());
        assertEquals(password, config.password());
        assertEquals(poolConfig, config.pool());
    }

    @Test
    @DisplayName("Creating config with username and password only")
    void testConfigWithCredentials() {
        String url = "jdbc:sqlserver://localhost:1433;databaseName=testdb";
        JdbcDriver driver = JdbcDriver.MsSqlServer;
        String username = "sa";
        String password = "SecurePassword123!";

        JdbcConnectionConfig config = new JdbcConnectionConfig(
            url, driver, username, password
        );

        assertEquals(url, config.url());
        assertEquals(driver, config.driver());
        assertEquals(username, config.username());
        assertEquals(password, config.password());
        assertNull(config.pool());
    }

    @Test
    @DisplayName("Creating config with pool configuration only")
    void testConfigWithPoolOnly() {
        String url = "jdbc:sqlite:memory";
        JdbcDriver driver = JdbcDriver.HSQLDB;
        JdbcDatabasePoolConfig poolConfig = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(5),
            Duration.ofMinutes(15),
            Duration.ZERO,
            15,
            3
        );

        JdbcConnectionConfig config = new JdbcConnectionConfig(
            url, driver, null, null, poolConfig
        );

        assertEquals(url, config.url());
        assertEquals(driver, config.driver());
        assertNull(config.username());
        assertNull(config.password());
        assertEquals(poolConfig, config.pool());
    }

    @Test
    @DisplayName("Record should implement equals() correctly")
    void testRecordEquality() {
        String url = "jdbc:sqlite:memory";
        JdbcDriver driver = JdbcDriver.HSQLDB;

        JdbcConnectionConfig config1 = new JdbcConnectionConfig(url, driver);
        JdbcConnectionConfig config2 = new JdbcConnectionConfig(url, driver);

        assertEquals(config1, config2);
    }

    @Test
    @DisplayName("Record should differentiate different configs in equals()")
    void testRecordInequality() {
        JdbcConnectionConfig config1 = new JdbcConnectionConfig(
            "jdbc:sqlite:memory",
            JdbcDriver.HSQLDB
        );
        JdbcConnectionConfig config2 = new JdbcConnectionConfig(
            "jdbc:sqlite:file.db",
            JdbcDriver.MsSqlServer
        );

        assertNotEquals(config1, config2);
    }

    @Test
    @DisplayName("Record should implement hashCode() consistently")
    void testRecordHashCode() {
        String url = "jdbc:sqlite:memory";
        JdbcDriver driver = JdbcDriver.HSQLDB;

        JdbcConnectionConfig config1 = new JdbcConnectionConfig(url, driver);
        JdbcConnectionConfig config2 = new JdbcConnectionConfig(url, driver);

        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    @DisplayName("Record should implement toString() with all fields")
    void testRecordToString() {
        JdbcConnectionConfig config = new JdbcConnectionConfig(
            "jdbc:sqlite:memory",
            JdbcDriver.HSQLDB,
            "user",
            "pass"
        );

        String toString = config.toString();
        assertTrue(toString.contains("JdbcConnectionConfig"));
        assertTrue(toString.contains("jdbc:sqlite:memory"));
    }

    @Test
    @DisplayName("Accessors should work as record components")
    void testRecordAccessors() {
        String url = "jdbc:sqlite:test.db";
        JdbcDriver driver = JdbcDriver.MsSqlServer;
        String username = "admin";
        String password = "secret";

        JdbcConnectionConfig config = new JdbcConnectionConfig(
            url, driver, username, password
        );

        // Test all accessors
        assertEquals(url, config.url());
        assertEquals(driver, config.driver());
        assertEquals(username, config.username());
        assertEquals(password, config.password());
        assertNull(config.pool());
    }

    @Test
    @DisplayName("Should accept different driver types")
    void testDifferentDriverTypes() {
        String urlMsSql = "jdbc:sqlserver://localhost:1433;databaseName=test";
        JdbcConnectionConfig configMsSql = new JdbcConnectionConfig(
            urlMsSql, JdbcDriver.MsSqlServer
        );

        String urlSqlite = "jdbc:sqlite:test.db";
        JdbcConnectionConfig configSqlite = new JdbcConnectionConfig(
            urlSqlite, JdbcDriver.HSQLDB
        );

        assertEquals(JdbcDriver.MsSqlServer, configMsSql.driver());
        assertEquals(JdbcDriver.HSQLDB, configSqlite.driver());
        assertNotEquals(configMsSql.driver(), configSqlite.driver());
    }
}
