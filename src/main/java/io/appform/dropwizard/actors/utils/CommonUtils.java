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

package io.appform.dropwizard.actors.utils;

import static io.appform.dropwizard.actors.common.Constants.MESSAGE_DELIVERY_ATTEMPT;
import static io.appform.dropwizard.actors.common.Constants.MESSAGE_PUBLISHED_TEXT;
import static io.appform.dropwizard.actors.common.Constants.MESSAGE_REPUBLISHED_TEXT;

import com.google.common.base.Strings;
import com.rabbitmq.client.AMQP;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommonUtils {

    public static boolean isEmpty(Collection<?> collection) {
        return null == collection || collection.isEmpty();
    }

    public static boolean isEmpty(Map<?, ?> map) {
        return null == map || map.isEmpty();
    }

    public static boolean isEmpty(String s) {
        return Strings.isNullOrEmpty(s);
    }

    public static boolean isRetriable(Set<String> retriableExceptions, Throwable exception) {
        return CommonUtils.isEmpty(retriableExceptions)
                || (null != exception
                && retriableExceptions.contains(exception.getClass().getSimpleName()));
    }

    public static AMQP.BasicProperties getEnrichedProperties(AMQP.BasicProperties properties) {
        return getEnrichedProperties(properties, 1);
    }

    public static AMQP.BasicProperties getEnrichedProperties(AMQP.BasicProperties properties, int deliveryAttempt) {
        HashMap<String, Object> enrichedHeaders = new HashMap<>();
        if (properties.getHeaders() != null) {
            enrichedHeaders.putAll(properties.getHeaders());
        }
        if(properties.getHeaders().containsKey(MESSAGE_PUBLISHED_TEXT)) {
            enrichedHeaders.put(MESSAGE_REPUBLISHED_TEXT, Instant.now().toEpochMilli());
        } else {
            enrichedHeaders.put(MESSAGE_PUBLISHED_TEXT, Instant.now().toEpochMilli());
        }
        enrichedHeaders.put(MESSAGE_DELIVERY_ATTEMPT, deliveryAttempt);
        return properties.builder()
                .headers(Collections.unmodifiableMap(enrichedHeaders))
                .build();
    }

}
