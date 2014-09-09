package io.trygvis.jz14.demo;

import com.impossibl.postgres.jdbc.PGDataSource;
import org.postgresql.ds.PGPoolingDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.util.Optional.ofNullable;

public class Db {

    public final String applicationName;
    public final boolean ng;

    public Db(String applicationName) {
        this.applicationName = applicationName;
        ng = false;
    }

    public Db(String applicationName, boolean ng) {
        this.applicationName = applicationName;
        this.ng = ng;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), jdbcUsername(), jdbcPassword());
    }

    private String jdbcPassword() {
        return ofNullable(System.getenv("password")).orElseGet(() -> System.getProperty("user.name"));
    }

    private String jdbcUsername() {
        return ofNullable(System.getenv("username")).orElseGet(() -> System.getProperty("user.name"));
    }

    private String jdbcDatabase() {
        return ofNullable(System.getenv("database")).orElseGet(() -> System.getProperty("user.name"));
    }

    private String jdbcUrl() {
        if (ng) {
            return "jdbc:pgsql://localhost/" + System.getProperty("user.name") + "?ApplicationName=" + applicationName;
        }
        return "jdbc:postgresql://localhost/" + System.getProperty("user.name") + "?ApplicationName=" + applicationName;
    }

    public DataSource dataSource() throws SQLException {
        if (!ng) {
            PGPoolingDataSource ds = new PGPoolingDataSource();
            ds.setUrl(jdbcUrl());
            ds.setUser(jdbcUsername());
            ds.setPassword(jdbcPassword());
            return ds;
        } else {
            PGDataSource ds = new PGDataSource();
            ds.setHost("localhost");
            ds.setUser(jdbcUsername());
            ds.setPassword(jdbcPassword());
            ds.setDatabase(jdbcDatabase());
            return ds;
        }
    }
}
