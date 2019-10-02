package com.amazonaws.dynamodb.stream.fanout.lambda;

import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.dynamodb.stream.fanout.dagger.FanoutComponent;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.dynamodb.stream.fanout.publisher.EventPublisher;
import com.amazonaws.dynamodb.stream.fanout.dagger.DaggerFanoutComponent;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

/**
 * The entry point for Lambda function to handle the requests.
 */
public class FanoutHandler implements RequestHandler<DynamodbEvent, Void> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final EventPublisher eventPublisher;

    static {
        OBJECT_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public FanoutHandler() {
        FanoutComponent fanoutComponent = DaggerFanoutComponent.builder().build();
        eventPublisher = fanoutComponent.getEventPublisher();
    }

    @Override
    public Void handleRequest(final DynamodbEvent dynamodbEvent, final Context context) {
        List<String> events = dynamodbEvent.getRecords()
                .stream()
                .map(this::toString)
                .collect(Collectors.toList());
        eventPublisher.publish(events);
        return null;
    }

    @SneakyThrows(JsonProcessingException.class)
    private String toString(final DynamodbEvent.DynamodbStreamRecord record) {
        return OBJECT_MAPPER.writeValueAsString(record);
    }
}
