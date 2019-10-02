package com.amazonaws.dynamodb.stream.fanout.publisher;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Implementation of {@link EventPublisher} using Amazon SQS SendMessage API.
 *
 * <p>It sends one SQS message for one DynamodbEvent.
 */
@Slf4j
@RequiredArgsConstructor
public class SqsEventPublisher implements EventPublisher {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private final SqsClient sqs;
  private final String queueUrl;

  @Override
  @SneakyThrows(JsonProcessingException.class)
  public void publish(final DynamodbEvent event) {
    log.debug("Sending events {} to SQS queue {}", event, queueUrl);
    sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(OBJECT_MAPPER.writeValueAsString(event))
                    .build());
  }
}
