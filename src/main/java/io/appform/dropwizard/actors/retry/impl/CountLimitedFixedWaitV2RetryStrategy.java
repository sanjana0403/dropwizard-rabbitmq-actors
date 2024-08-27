package io.appform.dropwizard.actors.retry.impl;

import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import io.appform.dropwizard.actors.retry.RetryStrategy;
import io.appform.dropwizard.actors.retry.config.CountLimitedFixedWaitRetryConfig;
import java.util.concurrent.Callable;

/**
 *
 * Limits retries
 *
 */
public class CountLimitedFixedWaitV2RetryStrategy extends RetryStrategy {
    @SuppressWarnings("unused")
    public CountLimitedFixedWaitV2RetryStrategy(CountLimitedFixedWaitRetryConfig config) {
        super(RetryerBuilder.<Boolean>newBuilder()
                .withStopStrategy(StopStrategies.stopAfterAttempt(1))
                .build());
    }

    @Override
    public boolean execute(Callable<Boolean> callable) {

        return true;
    }
}

