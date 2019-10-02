package com.amazonaws.dynamodb.stream.fanout.dagger;

import javax.inject.Singleton;

import com.amazonaws.dynamodb.stream.fanout.publisher.EventBridgePublisher;
import com.amazonaws.dynamodb.stream.fanout.publisher.EventBridgeRetryClient;
import com.amazonaws.dynamodb.stream.fanout.publisher.EventPublisher;
import com.amazonaws.dynamodb.stream.fanout.lambda.Env;
import com.amazonaws.dynamodb.stream.fanout.publisher.SqsEventPublisher;

import dagger.Module;
import dagger.Provides;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.time.Duration;

/**
 * Dagger wiring.
 */
@Module
public class FanoutModule {
    @Provides
    @Singleton
    public EventBridgeClient provideEventBridgeClient() {
        // Creating the DynamoDB client followed AWS SDK v2 best practice to improve Lambda performance:
        // https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/client-configuration-starttime.html
        return EventBridgeClient.builder()
                .region(Region.of(System.getenv("AWS_REGION")))
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallAttemptTimeout(Duration.ofSeconds(1))
                        .retryPolicy(RetryPolicy.builder().numRetries(10).build())
                        .build())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    @Provides
    @Singleton
    public SqsClient provideSqsClient() {
        // Creating the DynamoDB client followed AWS SDK v2 best practice to improve Lambda performance:
        // https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/client-configuration-starttime.html
        return SqsClient.builder()
                .region(Region.of(System.getenv("AWS_REGION")))
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .build();
    }

    @Provides
    @Singleton
    public EventPublisher provideEventPublisher(final EventBridgeClient eventBridge, final SqsClient sqs) {
        EventPublisher failedEventPublisher = new SqsEventPublisher(sqs, Env.getDlqUrl());
        EventBridgeRetryClient eventBridgeRetryClient = new EventBridgeRetryClient(eventBridge, Env.getMaxAttempt());
        return new EventBridgePublisher(eventBridgeRetryClient, failedEventPublisher,
                Env.getEventBusName());
    }
}