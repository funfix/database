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

import org.funfix.delayedqueue.jvm.JdbcDatabasePoolConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

/**
 * Tests for JdbcDatabasePoolConfig record.
 */
@DisplayName("JdbcDatabasePoolConfig Tests")
class JdbcDatabasePoolConfigTest {

    @Test
    @DisplayName("Creating pool config with all default values")
    void testDefaultPoolConfig() {
        JdbcDatabasePoolConfig config = new JdbcDatabasePoolConfig();

        assertEquals(Duration.ofSeconds(30), config.connectionTimeout());
        assertEquals(Duration.ofMinutes(10), config.idleTimeout());
        assertEquals(Duration.ofMinutes(30), config.maxLifetime());
        assertEquals(Duration.ZERO, config.keepaliveTime());
        assertEquals(10, config.maximumPoolSize());
        assertNull(config.minimumIdle());
        assertNull(config.leakDetectionThreshold());
        assertNull(config.initializationFailTimeout());
    }

    @Test
    @DisplayName("Creating pool config with custom timeout values")
    void testCustomTimeouts() {
        Duration connTimeout = Duration.ofSeconds(15);
        Duration idleTimeout = Duration.ofMinutes(5);
        Duration maxLifetime = Duration.ofMinutes(20);
        Duration keepalive = Duration.ofMinutes(2);

        JdbcDatabasePoolConfig config = new JdbcDatabasePoolConfig(
            connTimeout,
            idleTimeout,
            maxLifetime,
            keepalive,
            10,
            null,
            null,
            null
        );

        assertEquals(connTimeout, config.connectionTimeout());
        assertEquals(idleTimeout, config.idleTimeout());
        assertEquals(maxLifetime, config.maxLifetime());
        assertEquals(keepalive, config.keepaliveTime());
    }

    @Test
    @DisplayName("Creating pool config with custom pool size values")
    void testCustomPoolSizes() {
        Duration defaultConnTimeout = Duration.ofSeconds(30);
        Duration defaultIdleTimeout = Duration.ofMinutes(10);
        Duration defaultMaxLifetime = Duration.ofMinutes(30);
        Duration defaultKeepalive = Duration.ZERO;

        JdbcDatabasePoolConfig config = new JdbcDatabasePoolConfig(
            defaultConnTimeout,
            defaultIdleTimeout,
            defaultMaxLifetime,
            defaultKeepalive,
            20,  // maximumPoolSize
            5,   // minimumIdle
            null,
            null
        );

        assertEquals(20, config.maximumPoolSize());
        assertEquals(5, config.minimumIdle());
    }

    @Test
    @DisplayName("Creating pool config with all optional parameters")
    void testFullPoolConfig() {
        Duration connTimeout = Duration.ofSeconds(20);
        Duration idleTimeout = Duration.ofMinutes(8);
        Duration maxLifetime = Duration.ofMinutes(25);
        Duration keepalive = Duration.ofMinutes(1);
        Duration leakThreshold = Duration.ofMinutes(60);
        Duration initTimeout = Duration.ofSeconds(5);

        JdbcDatabasePoolConfig config = new JdbcDatabasePoolConfig(
            connTimeout,
            idleTimeout,
            maxLifetime,
            keepalive,
            15,
            3,
            leakThreshold,
            initTimeout
        );

        assertEquals(connTimeout, config.connectionTimeout());
        assertEquals(idleTimeout, config.idleTimeout());
        assertEquals(maxLifetime, config.maxLifetime());
        assertEquals(keepalive, config.keepaliveTime());
        assertEquals(15, config.maximumPoolSize());
        assertEquals(3, config.minimumIdle());
        assertEquals(leakThreshold, config.leakDetectionThreshold());
        assertEquals(initTimeout, config.initializationFailTimeout());
    }

    @Test
    @DisplayName("Record should implement equals() correctly")
    void testRecordEquality() {
        JdbcDatabasePoolConfig config1 = new JdbcDatabasePoolConfig();
        JdbcDatabasePoolConfig config2 = new JdbcDatabasePoolConfig();

        assertEquals(config1, config2);
    }

    @Test
    @DisplayName("Record should differentiate different pool configs in equals()")
    void testRecordInequality() {
        JdbcDatabasePoolConfig config1 = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            null,
            null,
            null
        );
        JdbcDatabasePoolConfig config2 = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(60),  // Different connection timeout
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            null,
            null,
            null
        );

        assertNotEquals(config1, config2);
    }

    @Test
    @DisplayName("Record should implement hashCode() consistently")
    void testRecordHashCode() {
        JdbcDatabasePoolConfig config1 = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            null,
            null,
            null
        );
        JdbcDatabasePoolConfig config2 = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            null,
            null,
            null
        );

        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    @DisplayName("Record should implement toString() with all fields")
    void testRecordToString() {
        JdbcDatabasePoolConfig config = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            5,
            null,
            null
        );

        String toString = config.toString();
        assertTrue(toString.contains("JdbcDatabasePoolConfig"),
            "toString should contain class name");
        assertTrue(toString.contains("10") || toString.contains("5"),
            "toString should contain pool size values");
    }

    @Test
    @DisplayName("Accessors should work as record components")
    void testRecordAccessors() {
        Duration connTimeout = Duration.ofSeconds(45);
        Duration idleTimeout = Duration.ofMinutes(12);
        Duration maxLifetime = Duration.ofMinutes(35);
        Duration keepalive = Duration.ofSeconds(30);

        JdbcDatabasePoolConfig config = new JdbcDatabasePoolConfig(
            connTimeout,
            idleTimeout,
            maxLifetime,
            keepalive,
            20,
            4,
            Duration.ofHours(1),
            Duration.ofSeconds(10)
        );

        // Test all accessors
        assertEquals(connTimeout, config.connectionTimeout());
        assertEquals(idleTimeout, config.idleTimeout());
        assertEquals(maxLifetime, config.maxLifetime());
        assertEquals(keepalive, config.keepaliveTime());
        assertEquals(20, config.maximumPoolSize());
        assertEquals(4, config.minimumIdle());
        assertEquals(Duration.ofHours(1), config.leakDetectionThreshold());
        assertEquals(Duration.ofSeconds(10), config.initializationFailTimeout());
    }

    @Test
    @DisplayName("Zero duration should be allowed for keepaliveTime")
    void testZeroDurationKeepalive() {
        JdbcDatabasePoolConfig config = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            null,
            null,
            null
        );

        assertEquals(Duration.ZERO, config.keepaliveTime());
    }

    @Test
    @DisplayName("Null values should be allowed for optional thresholds")
    void testNullOptionalValues() {
        JdbcDatabasePoolConfig config = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            null,
            null,
            null
        );

        assertNull(config.minimumIdle());
        assertNull(config.leakDetectionThreshold());
        assertNull(config.initializationFailTimeout());
    }

    @Test
    @DisplayName("Pool size values should be independent")
    void testPoolSizeIndependence() {
        // Test with maximumPoolSize larger than minimumIdle
        JdbcDatabasePoolConfig config1 = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            20,
            5,
            null,
            null
        );

        // Test with minimumIdle only
        JdbcDatabasePoolConfig config2 = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            2,
            null,
            null
        );

        // Test with default maximumPoolSize and custom minimumIdle
        JdbcDatabasePoolConfig config3 = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(30),
            Duration.ofMinutes(10),
            Duration.ofMinutes(30),
            Duration.ZERO,
            10,
            3,
            null,
            null
        );

        assertEquals(20, config1.maximumPoolSize());
        assertEquals(5, config1.minimumIdle());
        assertEquals(10, config2.maximumPoolSize());
        assertEquals(2, config2.minimumIdle());
        assertEquals(10, config3.maximumPoolSize());
        assertEquals(3, config3.minimumIdle());
    }

    @Test
    @DisplayName("Different timeout combinations should not be equal")
    void testDifferentTimeoutCombinations() {
        JdbcDatabasePoolConfig aggressiveConfig = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(10),
            Duration.ofMinutes(2),
            Duration.ofMinutes(15),
            Duration.ZERO,
            5,
            1,
            null,
            null
        );

        JdbcDatabasePoolConfig conservativeConfig = new JdbcDatabasePoolConfig(
            Duration.ofSeconds(60),
            Duration.ofMinutes(20),
            Duration.ofMinutes(60),
            Duration.ZERO,
            50,
            10,
            null,
            null
        );

        assertNotEquals(aggressiveConfig, conservativeConfig);
    }
}
