package io.trygvis.jz14.db;

import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.trygvis.jz14.db.RobustTimerTask.robustTimerTask;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.slf4j.LoggerFactory.getLogger;

public class DbListener implements Closeable {
    private final Logger log = getLogger(getClass());

    private final DbListenerConfig config;
    private final Timer timer;

    private final Listener listener;

    @FunctionalInterface
    public static interface NewItemCallback {
        void newItem(boolean wasNotified, Iterable<String> parameters) throws Exception;
    }

    public static final class PostgresConnection<T> implements Closeable {
        public final Connection sqlConnection;
        public final T underlying;

        public PostgresConnection(Connection sqlConnection, T underlying) {
            this.sqlConnection = sqlConnection;
            this.underlying = underlying;
        }

        @Override
        public final void close() {
            try {
                sqlConnection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public abstract static class DbListenerConfig {

        /**
         * Default scanner delay; 1 minute;
         */
        public static final long DEFAULT_SCANNER_DELAY = ms(1, MINUTES);

        /**
         * Default scanner period; 1 hour.
         */
        public static final long DEFAULT_SCANNER_PERIOD = ms(1, HOURS);

        /**
         * Default connection life check interval; 1 hour;
         */
        public static final long DEFAULT_LISTENER_CONNECTION_LIVE_CHECK_INTERVAL = ms(1, HOURS);

        /**
         * Default sleep time after failure; 1 second;
         */
        public static final long DEFAULT_LISTENER_SLEEP_INTERVAL = ms(200, MILLISECONDS);

        /**
         * Default sleep time after failure; 1 minute;
         */
        public static final long DEFAULT_LISTENER_FAILURE_SLEEP_INTERVAL = ms(1, MINUTES);

        /**
         * The name of the channel to listen on.
         */
        public final String name;

        /**
         * Should the scanner thread be started?
         */
        public final boolean runScanner;

        /**
         * Should the listener thread be started?
         */
        public final boolean runListener;

        /**
         * How long should the scanner wait after start before starting to scan.
         */
        public final long scannerDelay;

        /**
         * How often should the scanner wait between scans.
         */
        public long scannerPeriod;

        /**
         * How long should the listener sleep after each poll.
         */
        public long listenerSleepInterval;

        /**
         * How often should the listener do a live check of the connection.
         */
        public long listenerConnectionLiveCheckInterval;

        /**
         * How long should the listener sleep after a failure.
         */
        public long listenerFailureSleepInterval;

        /**
         * Configures a listener with the default parameters,
         */
        public DbListenerConfig(String name) {
            this(name, true, true, DEFAULT_SCANNER_DELAY, DEFAULT_SCANNER_PERIOD,
                    DEFAULT_LISTENER_SLEEP_INTERVAL, DEFAULT_LISTENER_CONNECTION_LIVE_CHECK_INTERVAL, DEFAULT_LISTENER_FAILURE_SLEEP_INTERVAL);
        }

        public DbListenerConfig(String name, boolean runScanner, boolean runListener, long scannerDelay, long scannerPeriod,
                                long listenerSleepInterval, long listenerConnectionLiveCheckInterval, long listenerFailureSleepInterval) {
            this.name = name;
            this.runScanner = runScanner;
            this.runListener = runListener;
            this.scannerDelay = scannerDelay;
            this.scannerPeriod = scannerPeriod;
            this.listenerSleepInterval = listenerSleepInterval;
            this.listenerConnectionLiveCheckInterval = listenerConnectionLiveCheckInterval;
            this.listenerFailureSleepInterval = listenerFailureSleepInterval;
        }

        public static long ms(long count, TimeUnit unit) {
            return MILLISECONDS.convert(count, unit);
        }
    }

    public static DbListener nativeDbListener(DbListenerConfig config, NewItemCallback callable,
                                              Supplier<PostgresConnection<org.postgresql.PGConnection>> connectionSupplier) throws SQLException {
        return new DbListener(config, callable, connectionSupplier, null);
    }

    public static DbListener ngDbListener(DbListenerConfig config, NewItemCallback callable,
                                          Supplier<PostgresConnection<com.impossibl.postgres.api.jdbc.PGConnection>> connectionSupplier) throws SQLException {
        return new DbListener(config, callable, null, connectionSupplier);
    }

    private DbListener(DbListenerConfig config, NewItemCallback callable,
                       Supplier<PostgresConnection<org.postgresql.PGConnection>> nativeSup,
                       Supplier<PostgresConnection<com.impossibl.postgres.api.jdbc.PGConnection>> ngSup) throws SQLException {
        this.config = config;

        log.info("DB Listener: {}", config.name);
        log.info(" Run scanner: {}", config.runScanner);
        log.info(" Run listener: {}", config.runListener);

        if (config.runScanner) {
            timer = new Timer("Timer \"" + config.name + "\"", true);

            timer.schedule(robustTimerTask(new NewItemCallbackCallable<>(callable)), config.scannerDelay, config.scannerPeriod);
        } else {
            timer = null;
        }

        if (config.runListener) {
            try (PostgresConnection<?> pg = (nativeSup != null ? nativeSup : ngSup).get()) {
                Connection c = pg.sqlConnection;
                if (c.getMetaData().getDatabaseProductName().toLowerCase().contains("postgresql")) {
                    if (nativeSup != null) {
                        listener = new NativeListener(config, nativeSup, callable);
                    } else {
                        listener = new NgListener(config, ngSup, callable);
                    }
                    Thread thread = new Thread(listener, "LISTEN \"" + config.name + "\"");
                    thread.start();
                } else {
                    log.info("Mail listener is configured to run, but the database isn't a PostgreSQL database");
                    listener = null;
                }
            }
        } else {
            listener = null;
        }
    }

    public void close() {
        log.info("Stopping DB listener {}", config.name);
        if (timer != null) {
            timer.cancel();
        }

        if (listener != null) {
            try {
                listener.close();
            } catch (IOException e) {
                log.warn("Exception while closing listener.", e);
            }
        }
    }

    private class NewItemCallbackCallable<A> implements Callable<A> {

        private final NewItemCallback callback;

        private NewItemCallbackCallable(NewItemCallback callback) {
            this.callback = callback;
        }

        @Override
        public A call() throws Exception {
            callback.newItem(false, Collections.emptyList());

            return null;
        }
    }
}
