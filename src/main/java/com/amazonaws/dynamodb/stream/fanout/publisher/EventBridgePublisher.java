package com.amazonaws.dynamodb.stream.fanout.publisher;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Implementation of {@link EventPublisher} using AWS EventBridge PutEvents API.
 *
 * <p>It publishes one DynamodbStreamRecord as one EventBridge event.
 */
@Slf4j
@RequiredArgsConstructor
public class EventBridgePublisher implements EventPublisher {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    static final String EVENT_SOURCE = "aws-dynamodb-stream-eventbridge-fanout";
    static final String EVENT_DETAIL_TYPE = "dynamodb-stream-event";
    private final EventBridgeRetryClient eventBridge;
    private final EventPublisher failedEventPublisher;
    private final String eventBusName;
    private final Clock clock;

    public EventBridgePublisher(final EventBridgeRetryClient eventBridge,
                          final EventPublisher failedEventPublisher,
                          final String eventBusName) {
        this(eventBridge, failedEventPublisher, eventBusName, Clock.systemUTC());
    }

    @Override
    public void publish(final DynamodbEvent event) {
        Instant time = Instant.now(clock);
        List<PutEventsRequestEntry> requestEntries = event.getRecords()
                .stream()
                .map(record ->
            PutEventsRequestEntry.builder()
                    .eventBusName(eventBusName)
                    .time(time)
                    .source(EVENT_SOURCE)
                    .detailType(EVENT_DETAIL_TYPE)
                    .detail(toString(record))
                    .resources(record.getEventSourceARN())
                    .build())
                .collect(Collectors.toList());

        List<PutEventsRequestEntry> failedEntries = eventBridge.putEvents(PutEventsRequest.builder()
                .entries(requestEntries)
                .build());

        if (!failedEntries.isEmpty()) {
            log.debug("Sending failed events {} to failed event publisher", failedEntries);
            failedEntries.forEach(this::publishFailedEvent);
        }
    }

    @SneakyThrows(JsonProcessingException.class)
    private String toString(final DynamodbEvent.DynamodbStreamRecord record) {
        return OBJECT_MAPPER.writeValueAsString(record);
    }

    @SneakyThrows(IOException.class)
    private void publishFailedEvent(final PutEventsRequestEntry entry) {
        DynamodbEvent.DynamodbStreamRecord record = OBJECT_MAPPER.readValue(entry.detail(), DynamodbEvent.DynamodbStreamRecord.class);
        DynamodbEvent failedEvent = new DynamodbEvent();
        failedEvent.setRecords(Collections.singletonList(record));
        failedEventPublisher.publish(failedEvent);
    }
}
