package io.trygvis.jz14.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;
import java.util.concurrent.Callable;

public abstract class RobustTimerTask extends TimerTask {
    private static final Logger log = LoggerFactory.getLogger(RobustTimerTask.class);

    private final Class<?> klass;

    private RobustTimerTask(Class<?> klass) {
        this.klass = klass;
    }

    protected RobustTimerTask() {
        this.klass = getClass();
    }

    public static TimerTask robustTimerTask(final TimerTask timerTask) {
        return new RobustTimerTask(timerTask.getClass()) {
            public void timerRun() throws Exception {
                timerTask.run();
            }
        };
    }

    public static <T> TimerTask robustTimerTask(final Callable<T> callable) {
        return new RobustTimerTask(callable.getClass()) {
            public void timerRun() throws Exception {
                callable.call();
            }
        };
    }

    public void run() {
        try {
            timerRun();
        } catch (Exception e) {
            log.error("Timer task " + klass.getName() + " failed.", e);
        }
    }

    public abstract void timerRun() throws Exception;
}
