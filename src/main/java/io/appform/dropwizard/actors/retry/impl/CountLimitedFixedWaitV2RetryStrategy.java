package io.appform.dropwizard.actors.retry.impl;

import static io.appform.dropwizard.actors.common.Constants.MESSAGE_DELIVERY_ATTEMPT;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.retry.RetryStrategy;
import io.appform.dropwizard.actors.retry.config.CountLimitedFixedWaitRetryConfig;
import io.appform.dropwizard.actors.utils.CommonUtils;
import io.dropwizard.util.Duration;
import java.io.IOException;
import java.util.concurrent.Callable;

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
    Channel retryPublishChannel;
    CountLimitedFixedWaitRetryConfig config;
    Message message;
    BasicProperties messageProperties;

    @SuppressWarnings("unused")
    public CountLimitedFixedWaitV2RetryStrategy(CountLimitedFixedWaitRetryConfig config, String retryQueue,
            String retryExchange, Channel channel, Message message, BasicProperties messageProperties, ObjectMapper mapper) {
        super(RetryerBuilder.<Boolean>newBuilder()
                .withStopStrategy(StopStrategies.stopAfterAttempt(1))
                .build());
        this.retryPublishChannel = channel;
        this.retryQueue = retryQueue;
        this.retryExchange = retryExchange;
        this.config = config;
        this.mapper = mapper;
        this.message = message;
        this.messageProperties = messageProperties;
    }

    @Override
    public boolean execute(Callable<Boolean> callable) throws IOException {
        int deliveryAttempt = messageProperties.getHeaders().containsKey(MESSAGE_DELIVERY_ATTEMPT)
                ? (int) messageProperties.getHeaders().get(MESSAGE_DELIVERY_ATTEMPT)
                : 0;
        if(deliveryAttempt!=0 && deliveryAttempt < config.getMaxAttempts()) {
            retryPublishChannel.basicPublish(retryExchange, retryQueue, getProperties(config.getWaitTime()), mapper.writeValueAsBytes(message));
            return true;
        }
        retryPublishChannel.basicPublish(sidelineExchange, sidelineQueue, getProperties(), mapper.writeValueAsBytes(message));
        return true;
    }

    private BasicProperties getProperties(Duration wait) {
        BasicProperties properties = CommonUtils.getEnrichedProperties(messageProperties);
        return properties.builder().expiration(String.valueOf(wait.toMilliseconds())).build();
    }

    private BasicProperties getProperties() {
        return CommonUtils.getEnrichedProperties(messageProperties);
    }
}

