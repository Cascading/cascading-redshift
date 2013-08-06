package org.pingles.cascading.redshift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;

public class ExecutorTimeoutCommitter implements ResourceCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorTimeoutCommitter.class);
    private final Integer minutesToWait;
    private Callable<Boolean> commitTask;

    public ExecutorTimeoutCommitter(Callable<Boolean> commitTask, Integer minutesToWait) {
        this.commitTask = commitTask;
        this.minutesToWait = minutesToWait;
    }

    public boolean commit() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        Future<Boolean> future = executorService.submit(commitTask);
        try {
            future.get(minutesToWait, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            LOGGER.warn("Commit interrupted", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            LOGGER.error("Execution exception", e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOGGER.warn("Timed out executing copy", e);
        }

        return true;
    }
}
