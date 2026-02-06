package org.funfix.delayedqueue.api;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;

final class MsSqlLocalDatabase {
    private static final String HOST = "localhost";
    private static final int PORT = 1433;
    private static final String USERNAME = "sa";

    private MsSqlLocalDatabase() {}

    static String adminPassword() {
        String password = System.getenv("MSSQL_ADMIN_PASSWORD");
        if (password == null || password.isBlank()) {
            throw new IllegalStateException(
                "MSSQL_ADMIN_PASSWORD must be set to run MS-SQL tests"
            );
        }
        return password;
    }

    static String createDatabase() throws Exception {
        String dbName = "dq_test_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = DriverManager.getConnection(masterJdbcUrl(), USERNAME, adminPassword());
             Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE DATABASE [" + dbName + "]");
        }
        return dbName;
    }

    static void dropDatabase(String dbName) throws Exception {
        try (Connection connection = DriverManager.getConnection(masterJdbcUrl(), USERNAME, adminPassword());
             Statement stmt = connection.createStatement()) {
            stmt.execute(
                "ALTER DATABASE [" + dbName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            );
            stmt.execute("DROP DATABASE [" + dbName + "]");
        }
    }

    static String jdbcUrl(String dbName) {
        return baseJdbcUrl() + ";databaseName=" + dbName;
    }

    static String baseJdbcUrl() {
        return "jdbc:sqlserver://" + HOST + ":" + PORT +
            ";encrypt=false;trustServerCertificate=true";
    }

    private static String masterJdbcUrl() {
        return baseJdbcUrl() + ";databaseName=master";
    }
}
