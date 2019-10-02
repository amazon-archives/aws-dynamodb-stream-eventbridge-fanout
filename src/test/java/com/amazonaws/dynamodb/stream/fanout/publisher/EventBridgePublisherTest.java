package com.amazonaws.dynamodb.stream.fanout.publisher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;

public class EventBridgePublisherTest {
    private static final String EVENT_BUS = UUID.randomUUID().toString();
    private static final String EVENT_RESOURCE = UUID.randomUUID().toString();
    private static final Instant NOW = Instant.now();
    @Mock
    private EventBridgeRetryClient eventBridge;
    @Mock
    private EventPublisher failedEventPublisher;

    private EventPublisher publisher;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        publisher = new EventBridgePublisher(eventBridge, failedEventPublisher,
                EVENT_BUS, EVENT_RESOURCE, Clock.fixed(NOW, ZoneId.systemDefault()));
    }

    @Test
    public void publish() {
        String record = UUID.randomUUID().toString();
        List<String> events = Collections.singletonList(record);
        when(eventBridge.putEvents(any(PutEventsRequest.class))).thenReturn(Collections.emptyList());

        publisher.publish(events);

        PutEventsRequest expected = PutEventsRequest.builder()
                .entries(PutEventsRequestEntry.builder()
                        .eventBusName(EVENT_BUS)
                        .time(NOW)
                        .source(EventBridgePublisher.EVENT_SOURCE)
                        .detailType(EventBridgePublisher.EVENT_DETAIL_TYPE)
                        .detail(record)
                        .resources(EVENT_RESOURCE)
                        .build())
                .build();
        verify(eventBridge).putEvents(expected);
        verifyZeroInteractions(failedEventPublisher);
    }

    @Test
    public void publish_failedEvents() {
        String record = UUID.randomUUID().toString();
        String failedRecord = "failed-" + UUID.randomUUID().toString();
        List<String> events = new ArrayList<>();
        events.add(record);
        events.add(failedRecord);
        PutEventsRequestEntry.Builder builder = PutEventsRequestEntry.builder()
                .eventBusName(EVENT_BUS)
                .time(NOW)
                .source(EventBridgePublisher.EVENT_SOURCE)
                .detailType(EventBridgePublisher.EVENT_DETAIL_TYPE)
                .resources(EVENT_RESOURCE);
        List<PutEventsRequestEntry> failedEntries = Collections.singletonList(builder
                .detail(failedRecord)
                .build());
        when(eventBridge.putEvents(any(PutEventsRequest.class))).thenReturn(failedEntries);

        publisher.publish(events);

        PutEventsRequest expected = PutEventsRequest.builder()
                .entries(builder.detail(record).build(),
                        builder.detail(failedRecord).build())
                .build();
        verify(eventBridge).putEvents(expected);
        verify(failedEventPublisher).publish(Collections.singletonList(failedRecord));
    }
}
