package org.pingles.cascading.redshift;

import java.util.concurrent.TimeUnit;

class Timeout {
    private final Integer delay;
    private final TimeUnit timeUnit;

    Timeout(Integer delay, TimeUnit timeUnit) {
        this.delay = delay;
        this.timeUnit = timeUnit;
    }

    Integer getDelay() {
        return delay;
    }

    TimeUnit getUnit() {
        return timeUnit;
    }
}
