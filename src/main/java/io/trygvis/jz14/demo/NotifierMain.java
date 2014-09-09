package io.trygvis.jz14.demo;

import org.slf4j.Logger;

import java.sql.Connection;
import java.util.Random;

import static org.slf4j.LoggerFactory.getLogger;

public class NotifierMain {
    Logger log = getLogger(getClass());
    Db db = new Db("notifier");
    Random r = new Random();

    public static void main(String[] args) throws Exception {
        new NotifierMain().main();
    }

    public void main() throws Exception {
        Connection c = db.getConnection();
//        c.setAutoCommit(false);

        int count = 1 + r.nextInt(9);

        for (int i = 0; i < count; i++) {
            c.createStatement().execute("NOTIFY mail_raw, '" + i + "';");
        }
//        c.commit();

        log.info("NOTIFY performed, count={}", count);

        c.close();
    }
}
