package com.amazonaws.dynamodb.stream.fanout.publisher;

import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class SqsPublisherTest {
    private static final String QUEUE_URL = UUID.randomUUID().toString();
    @Mock
    private SqsClient sqs;

    private EventPublisher publisher;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        publisher = new SqsEventPublisher(sqs, QUEUE_URL);
    }

    @Test
    public void publish() {
        String record = UUID.randomUUID().toString();
        List<String> events = Collections.singletonList(record);

        publisher.publish(events);

        SendMessageRequest expected = SendMessageRequest.builder()
                .queueUrl(QUEUE_URL)
                .messageBody(record)
                .build();
        verify(sqs).sendMessage(expected);
    }
}
