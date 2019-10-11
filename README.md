## AWS DynamoDB Stream EventBridge Fanout 

This repository contains a serverless app that forwards events from a [DynamoDB stream](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html) to an [Amazon EventBridge](https://aws.amazon.com/eventbridge/) event bus. 

The high-level architecture is as follows:

![Architecture Diagram](./images/architecture_diagram.png)

A common pattern within cloud architectures is to trigger consumers based on changes to data, and this app helps to facilitate this pattern when using AWS services. 

DynamoDB streams capture changes to items in a DynamoDB table, and can be used to trigger AWS Lambda functions or other consumers. However, there is a limit of two consumers per DynamoDB stream. This app attaches a single Lambda function to a source DynamoDB stream, which captures the stream events and publishes them to an Amazon EventBridge event bus, which can support up to *100 consumers*.

### Installation Steps
This app is published as a serverless application in the AWS Serverless Application Repository. You can install it using the following steps:
1. [Create an AWS account](https://portal.aws.amazon.com/gp/aws/developer/registration/index.html) if you do not already have one and login
1. Go to this app's page on the [Serverless Application Repository](https://serverlessrepo.aws.amazon.com/applications/arn:aws:serverlessrepo:us-east-1:646794253159:applications~aws-dynamodb-stream-eventbridge-fanout)
1. Provide the required parameters and click "Deploy"

#### Parameters
This app has the following parameters:
1. `DynamoDBStreamArn` (required) - The ARN of the source DynamoDB stream
1. `EventBusName` (optional) - The name of the event bus to create. Default: default
1. `EventBridgeMaxAttempt` (optional) - The max attempts to try to put events into the event bus. Default: 1

## License
This project is licensed under the Apache-2.0 License.