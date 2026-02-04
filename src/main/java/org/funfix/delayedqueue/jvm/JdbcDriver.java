package org.funfix.delayedqueue.jvm;

import org.jspecify.annotations.NullMarked;
import java.util.Optional;

/**
 * Sealed interface for JDBC drivers.
 */
@NullMarked
public sealed interface JdbcDriver permits
    JdbcDriver.MsSqlServer,
    JdbcDriver.Sqlite {

    String className();

    /**
     * Implementation of JdbcDriver for Microsoft SQL Server.
     */
    record MsSqlServer() implements JdbcDriver {
        public static final MsSqlServer INSTANCE = new MsSqlServer();

        @Override
        public String className() {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
    }

    record Sqlite() implements JdbcDriver {
        public static final Sqlite INSTANCE = new Sqlite();

        @Override
        public String className() {
            return "org.sqlite.JDBC";
        }
    }

    static Optional<JdbcDriver> of(final String className) {
        if (className.equalsIgnoreCase(MsSqlServer.INSTANCE.className())) {
            return Optional.of(MsSqlServer.INSTANCE);
        } else if (className.equalsIgnoreCase(Sqlite.INSTANCE.className())) {
            return Optional.of(Sqlite.INSTANCE);
        } else {
            return Optional.empty();
        }
    }
}
