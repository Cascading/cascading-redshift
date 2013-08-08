package org.pingles.cascading.redshift;

import org.junit.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import static org.junit.Assert.assertTrue;

public class ExecutorTimeoutCommitterTest {
    @Test
    public void shouldAllowFastTaskToCompleteExecution() {
        Callable<Boolean> quickTask = new Callable<Boolean>() {
            public Boolean call() throws Exception {
                return true;
            }
        };
        ExecutorTimeoutCommitter committer = new ExecutorTimeoutCommitter(quickTask, new Timeout(1, TimeUnit.SECONDS));
        assertTrue(committer.commit());
    }

    @Test
    public void shouldStopSlowTaskButRecordCompletion() {
        Callable<Boolean> quickTask = new Callable<Boolean>() {
            public Boolean call() throws Exception {
                Thread.sleep(5000);
                return true;
            }
        };
        ExecutorTimeoutCommitter committer = new ExecutorTimeoutCommitter(quickTask, new Timeout(1, TimeUnit.SECONDS));
        assertTrue(committer.commit());
    }
}
