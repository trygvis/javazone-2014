package io.trygvis.jz14.db;

import com.impossibl.postgres.api.jdbc.PGConnection;
import io.trygvis.jz14.db.DbListener.PostgresConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

public class NgConnectionSupplier implements Supplier<PostgresConnection<PGConnection>> {
    private final DataSource dataSource;

    public NgConnectionSupplier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public PostgresConnection<PGConnection> get() {
        try {
            Connection sqlConnection = dataSource.getConnection();
            PGConnection pgConnection = unwrap(sqlConnection);
            return new PostgresConnection<>(sqlConnection, pgConnection);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static PGConnection unwrap(Connection c) {
        if (c instanceof PGConnection) {
            return (PGConnection) c;
        }

        /* If you're using Spring, you need to add these two to properly unwrap the underlying PGConnection.
        if (c instanceof ConnectionHandle) {
            return unwrap(((ConnectionHandle) c).getInternalConnection());
        }
        if (c instanceof ConnectionProxy) {
            return unwrap(DataSourceUtils.getTargetConnection(c));
        }
        */
        Class<? extends Connection> klass = c.getClass();

        Class<?>[] interfaces = klass.getInterfaces();
        System.out.println("interfaces.length = " + interfaces.length);
        for (Class<?> anInterface : interfaces) {
            System.out.println("anInterface = " + anInterface);
        }

        throw new RuntimeException("Could not unwrap connection to a PGConnection: " + c.getClass());
    }
}
