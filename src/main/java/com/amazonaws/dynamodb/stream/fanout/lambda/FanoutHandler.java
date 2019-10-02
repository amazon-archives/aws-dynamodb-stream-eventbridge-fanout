package com.amazonaws.dynamodb.stream.fanout.lambda;

import com.amazonaws.dynamodb.stream.fanout.dagger.FanoutComponent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.dynamodb.stream.fanout.publisher.EventPublisher;
import com.amazonaws.dynamodb.stream.fanout.dagger.DaggerFanoutComponent;

/**
 * The entry point for Lambda function to handle the requests.
 */
public class FanoutHandler implements RequestHandler<DynamodbEvent, Void> {
    private final EventPublisher eventPublisher;

    public FanoutHandler() {
        FanoutComponent fanoutComponent = DaggerFanoutComponent.builder().build();
        eventPublisher = fanoutComponent.getEventPublisher();
    }

    @Override
    public Void handleRequest(final DynamodbEvent dynamodbEvent, final Context context) {
        eventPublisher.publish(dynamodbEvent);
        return null;
    }
}
