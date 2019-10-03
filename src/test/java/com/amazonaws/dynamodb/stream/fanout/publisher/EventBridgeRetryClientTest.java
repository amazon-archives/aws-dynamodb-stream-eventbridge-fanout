package com.amazonaws.dynamodb.stream.fanout.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

public class EventBridgeRetryClientTest {
    private static final int MAX_ATTEMPT = 3;
    @Mock
    private EventBridgeClient eventBridge;

    private EventBridgeRetryClient client;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        client = new EventBridgeRetryClient(eventBridge, MAX_ATTEMPT);
    }

    @Test
    public void putEvents_noFailure() {
        PutEventsRequest request = PutEventsRequest.builder()
                .build();
        PutEventsResponse response = PutEventsResponse.builder()
                .failedEntryCount(0)
                .build();
        when(eventBridge.putEvents(any(PutEventsRequest.class))).thenReturn(response);

        List<PutEventsRequestEntry> result = client.putEvents(request);

        assertThat(result).isEmpty();
        verify(eventBridge).putEvents(request);
    }

    @Test
    public void putEvents_retryThenNoFailure() {
        List<PutEventsRequestEntry> requestEntries = new ArrayList<>();
        requestEntries.add(PutEventsRequestEntry.builder()
                .detail("success entry")
                .build());
        PutEventsRequestEntry failedEntry = PutEventsRequestEntry.builder()
                .detail("failed entry")
                .build();
        requestEntries.add(failedEntry);
        PutEventsRequest request = PutEventsRequest.builder()
                .entries(requestEntries)
                .build();
        PutEventsRequest secondRequest = PutEventsRequest.builder()
                .entries(Collections.singletonList(failedEntry))
                .build();
        List<PutEventsResultEntry> resultEntries = new ArrayList<>();
        resultEntries.add(PutEventsResultEntry.builder().build());
        resultEntries.add(PutEventsResultEntry.builder()
                .errorCode("failed")
                .build());
        PutEventsResponse firstResponse = PutEventsResponse.builder()
                .failedEntryCount(1)
                .entries(resultEntries)
                .build();
        PutEventsResponse secondResponse = PutEventsResponse.builder()
                .failedEntryCount(0)
                .build();
        when(eventBridge.putEvents(any(PutEventsRequest.class)))
                .thenReturn(firstResponse)
                .thenReturn(secondResponse);

        List<PutEventsRequestEntry> result = client.putEvents(request);

        assertThat(result).isEmpty();
        ArgumentCaptor<PutEventsRequest> putEventsRequestArgumentCaptor = ArgumentCaptor.forClass(PutEventsRequest.class);
        verify(eventBridge, times(2)).putEvents(putEventsRequestArgumentCaptor.capture());
        List<PutEventsRequest> expected = new ArrayList<>();
        expected.add(request);
        expected.add(secondRequest);
        assertThat(putEventsRequestArgumentCaptor.getAllValues()).isEqualTo(expected);
    }

    @Test
    public void putEvents_maxAttempt() {
        List<PutEventsRequestEntry> requestEntries = new ArrayList<>();
        requestEntries.add(PutEventsRequestEntry.builder()
                .detail("success entry")
                .build());
        PutEventsRequestEntry failedEntry = PutEventsRequestEntry.builder()
                .detail("failed entry")
                .build();
        requestEntries.add(failedEntry);
        PutEventsRequest request = PutEventsRequest.builder()
                .entries(requestEntries)
                .build();
        PutEventsRequest failedRequest = PutEventsRequest.builder()
                .entries(Collections.singletonList(failedEntry))
                .build();
        List<PutEventsResultEntry> resultEntries = new ArrayList<>();
        resultEntries.add(PutEventsResultEntry.builder().build());
        PutEventsResultEntry failedResponseEntry = PutEventsResultEntry.builder()
                .errorCode("failed")
                .build();
        resultEntries.add(failedResponseEntry);
        PutEventsResponse response = PutEventsResponse.builder()
                .failedEntryCount(1)
                .entries(resultEntries)
                .build();
        PutEventsResponse failedResponse = PutEventsResponse.builder()
                .failedEntryCount(1)
                .entries(Collections.singletonList(failedResponseEntry))
                .build();
        when(eventBridge.putEvents(any(PutEventsRequest.class)))
                .thenReturn(response)
                .thenReturn(failedResponse)
                .thenReturn(failedResponse);

        List<PutEventsRequestEntry> result = client.putEvents(request);

        assertThat(result).isEqualTo(failedRequest.entries());
        ArgumentCaptor<PutEventsRequest> putEventsRequestArgumentCaptor = ArgumentCaptor.forClass(PutEventsRequest.class);
        verify(eventBridge, times(3)).putEvents(putEventsRequestArgumentCaptor.capture());
        List<PutEventsRequest> expected = new ArrayList<>();
        expected.add(request);
        expected.add(failedRequest);
        expected.add(failedRequest);
        assertThat(putEventsRequestArgumentCaptor.getAllValues()).isEqualTo(expected);
    }
}
