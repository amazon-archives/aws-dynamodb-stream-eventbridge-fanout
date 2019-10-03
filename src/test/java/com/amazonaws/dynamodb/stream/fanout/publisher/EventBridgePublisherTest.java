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

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;

public class EventBridgePublisherTest {
    private static final String EVENT_BUS = UUID.randomUUID().toString();
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
                EVENT_BUS, Clock.fixed(NOW, ZoneId.systemDefault()));
    }

    @Test
    public void publish() {
        String eventSourceARN = UUID.randomUUID().toString();
        when(eventBridge.putEvents(any(PutEventsRequest.class))).thenReturn(Collections.emptyList());
        DynamodbEvent event = new DynamodbEvent();
        DynamodbEvent.DynamodbStreamRecord record = new DynamodbEvent.DynamodbStreamRecord();
        record.setEventSourceARN(eventSourceARN);
        event.setRecords(Collections.singletonList(record));

        publisher.publish(event);

        String expectedDetail = "{\"eventSourceARN\":\"" + eventSourceARN + "\"}";
        PutEventsRequest expected = PutEventsRequest.builder()
                .entries(PutEventsRequestEntry.builder()
                        .eventBusName(EVENT_BUS)
                        .time(NOW)
                        .source(EventBridgePublisher.EVENT_SOURCE)
                        .detailType(EventBridgePublisher.EVENT_DETAIL_TYPE)
                        .detail(expectedDetail)
                        .resources(eventSourceARN)
                        .build())
                .build();
        verify(eventBridge).putEvents(expected);
        verifyZeroInteractions(failedEventPublisher);
    }

    @Test
    public void publish_failedEvents() {
        String eventSourceARN = UUID.randomUUID().toString();
        DynamodbEvent event = new DynamodbEvent();
        DynamodbEvent failedEvent = new DynamodbEvent();
        List<DynamodbEvent.DynamodbStreamRecord> records = new ArrayList<>();

        DynamodbEvent.DynamodbStreamRecord successRecord = new DynamodbEvent.DynamodbStreamRecord();
        successRecord.setEventSourceARN(eventSourceARN);
        successRecord.setEventName("Success");
        records.add(successRecord);
        String successRecordString = "{\"eventName\":\"Success\"," +
                "\"eventSourceARN\":\"" + eventSourceARN + "\"}";

        DynamodbEvent.DynamodbStreamRecord failedRecord = new DynamodbEvent.DynamodbStreamRecord();
        failedRecord.setEventSourceARN(eventSourceARN);
        failedRecord.setEventName("Failed");
        records.add(failedRecord);
        String failedRecordString = "{\"eventName\":\"Failed\"," +
                "\"eventSourceARN\":\"" + eventSourceARN + "\"}";
        event.setRecords(records);
        failedEvent.setRecords(Collections.singletonList(failedRecord));

        PutEventsRequestEntry.Builder builder = PutEventsRequestEntry.builder()
                .eventBusName(EVENT_BUS)
                .time(NOW)
                .source(EventBridgePublisher.EVENT_SOURCE)
                .detailType(EventBridgePublisher.EVENT_DETAIL_TYPE)
                .resources(eventSourceARN);
        List<PutEventsRequestEntry> failedEntries = Collections.singletonList(builder
                .detail(failedRecordString)
                .build());
        when(eventBridge.putEvents(any(PutEventsRequest.class))).thenReturn(failedEntries);

        publisher.publish(event);

        PutEventsRequest expected = PutEventsRequest.builder()
                .entries(builder.detail(successRecordString).build(),
                        builder.detail(failedRecordString).build())
                .build();
        verify(eventBridge).putEvents(expected);
        verify(failedEventPublisher).publish(failedEvent);
    }
}
