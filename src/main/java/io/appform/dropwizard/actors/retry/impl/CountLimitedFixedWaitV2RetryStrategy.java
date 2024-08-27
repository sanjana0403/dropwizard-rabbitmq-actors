package io.appform.dropwizard.actors.retry.impl;

import static io.appform.dropwizard.actors.common.Constants.MESSAGE_DELIVERY_ATTEMPT;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.base.utils.NamingUtils;
import io.appform.dropwizard.actors.retry.RetryStrategy;
import io.appform.dropwizard.actors.retry.config.CountLimitedFixedWaitRetryConfig;
import io.appform.dropwizard.actors.utils.CommonUtils;
import io.dropwizard.util.Duration;
import java.io.IOException;
import java.util.concurrent.Callable;
import javax.ws.rs.NameBinding;

/**
 *
 * Limits retries
 *
 */
public class CountLimitedFixedWaitV2RetryStrategy<Message> extends RetryStrategy {

    String retryQueue;
    String retryExchange;
    String sidelineQueue;
    String sidelineExchange;
    ObjectMapper mapper;
    CountLimitedFixedWaitRetryConfig config;

    @SuppressWarnings("unused")
    public CountLimitedFixedWaitV2RetryStrategy(CountLimitedFixedWaitRetryConfig config, String queue,
            String exchange, ObjectMapper mapper) {
        super(RetryerBuilder.<Boolean>newBuilder()
                .withStopStrategy(StopStrategies.stopAfterAttempt(1))
                .build());
        this.retryQueue = NamingUtils.getRetry(queue);
        this.retryExchange = NamingUtils.getRetry(exchange);
        this.sidelineQueue = NamingUtils.getSideline(queue);
        this.sidelineExchange = NamingUtils.getSideline(exchange);
        this.config = config;
        this.mapper = mapper;
    }

    @Override
    public boolean execute(AMQP.BasicProperties properties, byte[] body, Channel retryChannel) throws IOException {
        int deliveryAttempt = properties.getHeaders().containsKey(MESSAGE_DELIVERY_ATTEMPT)
                ? (int) properties.getHeaders().get(MESSAGE_DELIVERY_ATTEMPT)
                : 0;
        if(deliveryAttempt!=0 && deliveryAttempt < config.getMaxAttempts()) {
            retryChannel.basicPublish(retryExchange, retryQueue, getProperties(config.getWaitTime(), properties), body);
            return true;
        }
        retryChannel.basicPublish(sidelineExchange, sidelineQueue, getProperties(properties), body);
        return true;
    }

    private BasicProperties getProperties(Duration wait, AMQP.BasicProperties properties) {
        BasicProperties enrichedProperties = CommonUtils.getEnrichedProperties(properties);
        return enrichedProperties.builder().expiration(String.valueOf(wait.toMilliseconds())).build();
    }

    private BasicProperties getProperties(AMQP.BasicProperties properties) {
        return CommonUtils.getEnrichedProperties(properties);
    }
}

