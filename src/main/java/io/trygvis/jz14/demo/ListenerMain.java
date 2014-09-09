package io.trygvis.jz14.demo;

import io.trygvis.jz14.db.DbListener;
import io.trygvis.jz14.db.DbListener.DbListenerConfig;
import io.trygvis.jz14.db.NativeConnectionSupplier;
import io.trygvis.jz14.db.NgConnectionSupplier;
import org.slf4j.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;

public class ListenerMain implements DbListener.NewItemCallback {
    Logger log = getLogger(getClass());

    public void main(boolean useNative) throws Exception {
        DbListener listener;
        if (useNative) {
            Db db = new Db("listener");
            listener = DbListener.nativeDbListener(
                    new DbListenerConfigForListenerMain("mail_raw"),
                    this, new NativeConnectionSupplier(db.dataSource())
            );
        } else {
            Db db = new Db("listener", true);
            listener = DbListener.ngDbListener(
                    new DbListenerConfigForListenerMain("mail_raw"),
                    this, new NgConnectionSupplier(db.dataSource())
            );
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                listener.close();
            }
        });

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

    @Override
    public void newItem(boolean wasNotified, Iterable<String> parameters) throws Exception {
        log.info("ListenerMain.newItem: wasNotified={}, parameters={}", wasNotified, parameters);
    }

    private class DbListenerConfigForListenerMain extends DbListenerConfig {
        public DbListenerConfigForListenerMain(String name) {
            super(name);
            listenerFailureSleepInterval = ms(5, SECONDS);
            listenerConnectionLiveCheckInterval = ms(5, SECONDS);
        }
    }
}
