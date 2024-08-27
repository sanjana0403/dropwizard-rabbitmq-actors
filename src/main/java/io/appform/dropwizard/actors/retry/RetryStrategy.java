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

import com.github.rholder.retry.Retryer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.util.concurrent.Callable;

/**
 * Baqse for all retry strategies
 */
public abstract class RetryStrategy {
    private final Retryer<Boolean> retryer;

    protected RetryStrategy(Retryer<Boolean> retryer) {
        this.retryer = retryer;
    }

    public boolean execute(Callable<Boolean> callable) throws Exception {
        return retryer.call(callable);
    }

    public boolean execute(AMQP.BasicProperties properties, byte[] body, Channel retryChannel) throws Exception {
        return true;
    }
}
