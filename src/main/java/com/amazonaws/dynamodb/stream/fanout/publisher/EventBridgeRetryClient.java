package com.amazonaws.dynamodb.stream.fanout.publisher;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

/**
 * This is a client that retries AWS EventBridge APIs
 */
@Slf4j
@RequiredArgsConstructor
public class EventBridgeRetryClient {
  private final EventBridgeClient eventBridge;
  private final int maxAttempt;

  /**
   * Call AWS EventBridge PutEvents API with retries.
   *
   * <p>PutEvents API puts a list of events. Some of them may fail.
   * The API returns information on the status of each event.
   * This method only retries the failed events until the maximum attempt count is met.
   *
   * @param request PutEvents API request.
   * @return a list of PutEventsRequestEntry that failed after maximum attempts.
   */
  public List<PutEventsRequestEntry> putEvents(final PutEventsRequest request) {
    PutEventsRequest requestCopy = request;
    int attemptCount = 0;
    while(attemptCount < maxAttempt) {
      log.debug("Attempt {} to put events {}", attemptCount + 1, requestCopy);
      PutEventsResponse response = eventBridge.putEvents(requestCopy);

      if (response.failedEntryCount() == 0) {
        return Collections.emptyList();
      }

      List<PutEventsRequestEntry> requestEntries = requestCopy.entries();
      List<PutEventsResultEntry> resultEntries = response.entries();

      List<PutEventsRequestEntry> failedEntries = IntStream
              .range(0, resultEntries.size())
              .filter(i -> resultEntries.get(i).errorCode() != null)
              .mapToObj(requestEntries::get)
              .collect(Collectors.toList());

      requestCopy = PutEventsRequest.builder()
              .entries(failedEntries)
              .build();

      attemptCount++;
    }
    return requestCopy.entries();
  }
}
