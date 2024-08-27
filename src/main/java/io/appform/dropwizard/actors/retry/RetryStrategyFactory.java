/*
 * Copyright (c) 2019 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.appform.dropwizard.actors.retry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.retry.config.CountLimitedExponentialWaitRetryConfig;
import io.appform.dropwizard.actors.retry.config.CountLimitedFixedWaitRetryConfig;
import io.appform.dropwizard.actors.retry.config.CountLimitedFixedWaitRetryConfigV2;
import io.appform.dropwizard.actors.retry.config.CountLimitedIncrementalWaitRetryConfig;
import io.appform.dropwizard.actors.retry.config.NoRetryConfig;
import io.appform.dropwizard.actors.retry.config.RetryConfig;
import io.appform.dropwizard.actors.retry.config.TimeLimitedExponentialWaitRetryConfig;
import io.appform.dropwizard.actors.retry.config.TimeLimitedFixedWaitRetryConfig;
import io.appform.dropwizard.actors.retry.config.TimeLimitedIncrementalWaitRetryConfig;
import io.appform.dropwizard.actors.retry.impl.CountLimitedExponentialWaitRetryStrategy;
import io.appform.dropwizard.actors.retry.impl.CountLimitedFixedWaitRetryStrategy;
import io.appform.dropwizard.actors.retry.impl.CountLimitedFixedWaitV2RetryStrategy;
import io.appform.dropwizard.actors.retry.impl.CountLimitedIncrementalWaitRetryStrategy;
import io.appform.dropwizard.actors.retry.impl.NoRetryStrategy;
import io.appform.dropwizard.actors.retry.impl.TimeLimitedExponentialWaitRetryStrategy;
import io.appform.dropwizard.actors.retry.impl.TimeLimitedFixedWaitRetryStrategy;
import io.appform.dropwizard.actors.retry.impl.TimeLimitedIncrementalWaitRetryStrategy;
import java.util.Calendar;
import java.util.concurrent.Callable;

/**
 * Creates strategy based on config
 */
public class RetryStrategyFactory {
    public RetryStrategy create(RetryConfig config, String queue, String exchange, ObjectMapper objectMapper) {
        switch (config.getType()) {
            case NO_RETRY:
                return new NoRetryStrategy(NoRetryConfig.class.cast(config));
            case TIME_LIMITED_EXPONENTIAL_BACKOFF:
                return new TimeLimitedExponentialWaitRetryStrategy(TimeLimitedExponentialWaitRetryConfig.class.cast(config));
            case TIME_LIMITED_INCREMENTAL_WAIT:
                return new TimeLimitedIncrementalWaitRetryStrategy(TimeLimitedIncrementalWaitRetryConfig.class.cast(config));
            case TIME_LIMITED_FIXED_WAIT:
                return new TimeLimitedFixedWaitRetryStrategy(TimeLimitedFixedWaitRetryConfig.class.cast(config));
            case COUNT_LIMITED_EXPONENTIAL_BACKOFF:
                return new CountLimitedExponentialWaitRetryStrategy(CountLimitedExponentialWaitRetryConfig.class.cast(config));
            case COUNT_LIMITED_INCREMENTAL_WAIT:
                return new CountLimitedIncrementalWaitRetryStrategy(CountLimitedIncrementalWaitRetryConfig.class.cast(config));
            case COUNT_LIMITED_FIXED_WAIT:
                return new CountLimitedFixedWaitRetryStrategy(CountLimitedFixedWaitRetryConfig.class.cast(config));
            case COUNT_LIMITED_FIXED_WAIT_V2:
                return new CountLimitedFixedWaitV2RetryStrategy((CountLimitedFixedWaitRetryConfigV2) config, queue, exchange, objectMapper);
        }
        return null;
    }

}
