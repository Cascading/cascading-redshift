package org.pingles.cascading.redshift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;

public class ExecutorTimeoutCommitter implements ResourceCommitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorTimeoutCommitter.class);
    private final Timeout timeout;
    private Callable<Boolean> commitTask;

    public ExecutorTimeoutCommitter(Callable<Boolean> commitTask, Timeout timeout) {
        this.commitTask = commitTask;
        this.timeout = timeout;
    }

    public boolean commit() {
        ExecutorService executorService = Executors.newCachedThreadPool();
        Future<Boolean> future = executorService.submit(commitTask);
        Boolean result = true;
        try {
            LOGGER.info("Submitting commit task with timeout: {}", timeout.toString());
            result = future.get(timeout.getDelay(), timeout.getUnit());
        } catch (InterruptedException e) {
            LOGGER.warn("Commit interrupted", e);
        } catch (ExecutionException e) {
            LOGGER.error("Execution exception", e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            LOGGER.warn("Timed out executing copy. Assuming success.", e);
        }

        LOGGER.info("Shutting down now.");
        executorService.shutdownNow();

        return result;
    }
}
