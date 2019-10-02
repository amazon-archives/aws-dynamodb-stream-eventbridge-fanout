package com.amazonaws.dynamodb.stream.fanout.publisher;

import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Implementation of {@link EventPublisher} using Amazon SQS SendMessage API.
 */
@Slf4j
@RequiredArgsConstructor
public class SqsEventPublisher implements EventPublisher {
  private final SqsClient sqs;
  private final String queueUrl;

  @Override
  public void publish(final List<String> events) {
    log.debug("Sending events {} to SQS queue {}", events, queueUrl);
    events.forEach(event ->
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(event)
                    .build()));
  }
}
