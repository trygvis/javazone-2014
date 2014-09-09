package io.trygvis.jz14.demo;

import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Random;

import static org.slf4j.LoggerFactory.getLogger;

public class InserterMain {
    Logger log = getLogger(getClass());
    Db db = new Db("inserter");
    Random r = new Random();

    public static void main(String[] args) throws Exception {
        new InserterMain().main();
    }

    public void main() throws Exception {
        Connection c = db.getConnection();
        c.setAutoCommit(false);

        PreparedStatement stmt = c.prepareStatement("INSERT INTO mail_raw_t(raw) VALUES(?)");
        int count = 1 + r.nextInt(9);

        for (int i = 0; i < count; i++) {
            stmt.setString(1, "mail #" + i);
            stmt.execute();
        }
        c.commit();

        log.info("INSERT performed, count={}", count);

        c.close();
    }
}
