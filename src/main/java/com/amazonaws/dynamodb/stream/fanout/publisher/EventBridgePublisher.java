package com.amazonaws.dynamodb.stream.fanout.publisher;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of {@link EventPublisher} using AWS EventBridge PutEvents API.
 */
@Slf4j
@RequiredArgsConstructor
public class EventBridgePublisher implements EventPublisher {
    static final String EVENT_SOURCE = "aws-dynamodb-stream-eventbridge-fanout";
    static final String EVENT_DETAIL_TYPE = "dynamodb-stream-event";
    private final EventBridgeRetryClient eventBridge;
    private final EventPublisher failedEventPublisher;
    private final String eventBusName;
    private final String eventResource;
    private final Clock clock;

    public EventBridgePublisher(final EventBridgeRetryClient eventBridge,
                          final EventPublisher failedEventPublisher,
                          final String eventBusName,
                          final String eventResource) {
        this(eventBridge, failedEventPublisher, eventBusName, eventResource, Clock.systemUTC());
    }

    @Override
    public void publish(final List<String> events) {
        Instant time = Instant.now(clock);
        List<PutEventsRequestEntry> requestEntries = events
                .stream()
                .map(record ->
            PutEventsRequestEntry.builder()
                    .eventBusName(eventBusName)
                    .time(time)
                    .source(EVENT_SOURCE)
                    .detailType(EVENT_DETAIL_TYPE)
                    .detail(record)
                    .resources(eventResource)
                    .build())
                .collect(Collectors.toList());

        List<PutEventsRequestEntry> failedEntries = eventBridge.putEvents(PutEventsRequest.builder()
                .entries(requestEntries)
                .build());

        if (!failedEntries.isEmpty()) {
            log.info("Sending failed events {} to failed event publisher", failedEntries);
            failedEventPublisher.publish(failedEntries.stream()
                    .map(PutEventsRequestEntry::detail)
                    .collect(Collectors.toList()));
        }
    }
}
