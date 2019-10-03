package com.amazonaws.dynamodb.stream.fanout.lambda;

/**
 * Utility class to get environment variables.
 */
public final class Env {
  public static String getEventBusName() {
    return getEnvValue("EVENT_BUS_NAME");
  }

  public static String getDlqUrl() {
    return getEnvValue("DLQ_URL");
  }

  public static int getMaxAttempt() {
    return Integer.parseInt(getEnvValue("MAX_ATTEMPT"));
  }

  private static String getEnvValue(final String name) {
    return System.getenv(name);
  }
}
