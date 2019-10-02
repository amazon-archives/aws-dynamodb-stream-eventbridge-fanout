package com.amazonaws.dynamodb.stream.fanout.publisher;

import java.util.List;

/**
 * Publishing events.
 */
public interface EventPublisher {
    /**
     * Publish a list of events that have been deseriazlied into String.
     * @param events a list of String events.
     */
    void publish(List<String> events);
}
