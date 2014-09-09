package io.trygvis.jz14.demo;

import io.trygvis.jz14.db.DbListener;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Kills all connections from {@link io.trygvis.jz14.demo.ListenerMain}.
 * <p/>
 * Note that this is slightly different from a postgresql backend failing, as the client will be told that it was
 * killed, but the effect in this code is the same.
 */
public class KillListenerMain implements DbListener.NewItemCallback {
    Logger log = getLogger(getClass());
    Db db = new Db("killer");

    public static void main(String[] args) throws Exception {
        new KillListenerMain().main();
    }

    private void main() throws Exception {
        Connection c = db.getConnection();
        Statement s = c.createStatement();

        ResultSet rs = s.executeQuery("SELECT pid FROM pg_stat_activity WHERE usename=user AND application_name='listener'");
        if (!rs.next()) {
            System.out.println("Couldn't find any listener");
        } else {
            List<Integer> pids = new ArrayList<>();

            do {
                int pid = rs.getInt("pid");
                pids.add(pid);
            } while (rs.next());

            for (Integer pid : pids) {
                System.out.println("Killing " + pid);
                ResultSet rs2 = s.executeQuery("select pg_terminate_backend(" + pid + ")");
                rs2.next();
                System.out.println("rs2.getBoolean(1) = " + rs2.getBoolean(1));
            }
        }

        c.close();
    }

    @Override
    public void newItem(boolean wasNotified, Iterable<String> parameters) throws Exception {
        log.info("ListenerMain.newItem: wasNotified={}, parameters={}", wasNotified, parameters);
    }
}
