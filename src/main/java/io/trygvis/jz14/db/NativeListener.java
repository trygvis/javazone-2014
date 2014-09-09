package io.trygvis.jz14.db;

import io.trygvis.jz14.db.DbListener.PostgresConnection;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.trygvis.jz14.db.DbListener.DbListenerConfig;
import static io.trygvis.jz14.db.DbListener.NewItemCallback;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;

class NativeListener implements Listener {
    private final Logger log = getLogger(getClass());

    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final DbListenerConfig config;
    private final Supplier<PostgresConnection<PGConnection>> connectionSupplier;
    private final NewItemCallback callback;

    private PostgresConnection<PGConnection> c;
    private long lastLiveCheck;

    public NativeListener(DbListenerConfig config, Supplier<PostgresConnection<PGConnection>> connectionSupplier, NewItemCallback callback) {
        this.config = config;
        this.connectionSupplier = connectionSupplier;
        this.callback = callback;
    }

    public synchronized void close() {
        shouldRun.set(false);
        notifyAll();
    }

    @Override
    public void run() {
        while (shouldRun.get()) {
            try {
                doIt();
            } catch (Throwable e) {
                log.error("doIt failed", e);
                try {
                    sleep(config.listenerFailureSleepInterval);
                } catch (InterruptedException ex2) {
                    if (!shouldRun.get()) {
                        log.error("Got interrupted.", ex2);
                    }
                }
            }
        }
    }

    private void doIt() throws Exception {

        if (c == null) {
            log.debug("Connecting to database");

            try {
                c = connectAndListen();
            } catch (Throwable ex) {
                onError("Unable to connect to database", ex);
                return;
            }
        }

        long now = currentTimeMillis();

        if (now - lastLiveCheck > config.listenerConnectionLiveCheckInterval) {
            log.debug("Doing live check");
            try {
                doLiveCheck();
                lastLiveCheck = now;
            } catch (Exception ex) {
                onError("Live check failed", ex);
                return;
            }
        }

        PGNotification[] notifications;
        try {
            notifications = c.underlying.getNotifications();
        } catch (SQLException e) {
            onError("Error while checking for notifications on connection.", e);
            return;
        }

        if (notifications != null) {
            List<String> strings = new ArrayList<>();
            for (PGNotification notification : notifications) {
                String parameter = notification.getParameter();
                if (parameter == null) {
                    continue;
                }

                parameter = parameter.trim();

                if (!parameter.isEmpty()) {
                    strings.add(parameter);
                }
            }

            log.debug("Got notification: parameters={}", strings);

            try {
                callback.newItem(true, strings);
            } catch (Exception ex) {
                onError("Notification handler failed", ex);
                return;
            }
        }

        sleep(config.listenerSleepInterval);
    }

    private void onError(String msg, Throwable ex) throws InterruptedException {
        log.info(msg, ex);

        // Do a last attempt at closing the connection, the connection pool might appreciate it.
        if (c != null) {
            try {
                c.close();
            } catch (Throwable e) {
                log.info("Exception when closing connection after error", e);
            }
        }
        c = null;
        log.info("Sleeping for {} ms after failure", config.listenerFailureSleepInterval);
        sleep(config.listenerFailureSleepInterval);
        log.info("Resuming work after failure");
    }

    private PostgresConnection<PGConnection> connectAndListen() throws SQLException {
        PostgresConnection<PGConnection> c = connectionSupplier.get();
        c.sqlConnection.setAutoCommit(true);

        lastLiveCheck = currentTimeMillis();

        PreparedStatement s = c.sqlConnection.prepareStatement("LISTEN \"" + config.name + "\"");
        s.execute();
        s.close();
        return c;
    }

    private void doLiveCheck() throws SQLException {
        PreparedStatement s = c.sqlConnection.prepareStatement("SELECT 1");
        s.executeQuery().close();
        s.close();
    }
}
