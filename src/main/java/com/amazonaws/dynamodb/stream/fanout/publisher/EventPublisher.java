package com.amazonaws.dynamodb.stream.fanout.publisher;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

/**
 * Publishing event.
 */
public interface EventPublisher {
    /**
     * Publish DynamoDB stream event.
     * @param event DynamoDB stream event.
     */
    void publish(DynamodbEvent event);
}
