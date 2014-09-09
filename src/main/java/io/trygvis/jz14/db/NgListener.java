package io.trygvis.jz14.db;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import io.trygvis.jz14.db.DbListener.PostgresConnection;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.trygvis.jz14.db.DbListener.DbListenerConfig;
import static io.trygvis.jz14.db.DbListener.NewItemCallback;
import static java.lang.Thread.sleep;
import static org.slf4j.LoggerFactory.getLogger;

class NgListener implements Listener, PGNotificationListener {
    private final Logger log = getLogger(getClass());

    private final AtomicBoolean shouldRun = new AtomicBoolean(true);
    private final DbListenerConfig config;
    private final Supplier<PostgresConnection<PGConnection>> connectionSupplier;
    private final NewItemCallback callback;

    private PostgresConnection<PGConnection> c;

    public NgListener(DbListenerConfig config, Supplier<PostgresConnection<PGConnection>> connectionSupplier, NewItemCallback callback) {
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

        log.debug("Doing live check");
        try {
            doLiveCheck();
        } catch (Exception ex) {
            onError("Live check failed", ex);
            return;
        }

        sleep(config.listenerConnectionLiveCheckInterval);
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

        c.underlying.addNotificationListener(this);

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

    @Override
    public void notification(int processId, String channelName, String payload) {
        try {
            callback.newItem(true, Collections.singletonList(payload));
        } catch (Exception e) {
            log.warn("Exception while processing callback, payload=" + payload, e);
        }
    }
}
