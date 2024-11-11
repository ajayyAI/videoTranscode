import { ECSClient, RunTaskCommand } from "@aws-sdk/client-ecs";
import {
    SQSClient,
    ReceiveMessageCommand,
    DeleteMessageCommand,
    ChangeMessageVisibilityCommand
} from '@aws-sdk/client-sqs';
import type { S3Event } from 'aws-lambda';

// Environment validation at startup
const requiredEnvVars = [
    'AWS_ACCESS_KEY_ID',
    'AWS_SECRET_ACCESS_KEY',
    'AWS_SQS_URL',
    'AWS_ECS_TASK_DEFINITION',
    'AWS_ECS_CLUSTER',
    'AWS_ECS_SUBNETS',
    'AWS_ECS_SECURITY_GROUP',
    'AWS_ECS_CONTAINER_NAME',
    'AWS_S3_PROD_BUCKET'
] as const;

function validateEnvironment(): void {
    const missing = requiredEnvVars.filter(key => !process.env[key]);
    if (missing.length > 0) {
        console.error(`Missing required environment variables: ${missing.join(', ')}`);
        process.exit(1);
    }
}

// Constants
const REGION = 'ap-south-1';
const MAX_RETRIES = 3;
const BATCH_SIZE = 5;
const WAIT_TIME_SECONDS = 5;
const VISIBILITY_TIMEOUT = 900; // 15 minutes

// AWS Clients
const sqsClient = new SQSClient({
    region: REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID as string,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY as string
    },
});

const ecsClient = new ECSClient({
    region: REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID as string,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY as string
    }
});

// Helper functions
async function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function retryWithBackoff<T>(
    operation: () => Promise<T>,
    retries: number = MAX_RETRIES
): Promise<T> {
    for (let i = 0; i < retries; i++) {
        try {
            return await operation();
        } catch (error) {
            if (i === retries - 1) throw error;
            const delay = Math.min(1000 * Math.pow(2, i), 5000);
            console.warn(`Retry ${i + 1}/${retries} after ${delay}ms`, error);
            await sleep(delay);
        }
    }
    throw new Error('Retry failed');
}

// Message processing functions
async function validateAndParseEvent(body: string): Promise<S3Event> {
    try {
        const event = JSON.parse(body) as S3Event;

        // Handle test events
        if ("Service" in event && "Event" in event) {
            if (event.Event === "s3:TestEvent") {
                throw new Error("Test event received");
            }
        }

        return event;
    } catch (error) {
        console.error('Event validation failed:', error);
        throw error;
    }
}

async function validateS3Key(key: string): Promise<boolean> {
    // Validate the expected format: courses/{courseId}/videos/{filename}
    const keyPattern = /^courses\/[\w-]+\/videos\/[\w-]+\.(mp4|mov|avi)$/i;
    return keyPattern.test(key);
}

async function launchEcsTask(bucketName: string, objectKey: string): Promise<void> {
    const command = new RunTaskCommand({
        taskDefinition: process.env.AWS_ECS_TASK_DEFINITION as string,
        cluster: process.env.AWS_ECS_CLUSTER as string,
        launchType: "FARGATE",
        networkConfiguration: {
            awsvpcConfiguration: {
                subnets: (process.env.AWS_ECS_SUBNETS as string).split(','),
                securityGroups: [process.env.AWS_ECS_SECURITY_GROUP as string],
                assignPublicIp: "ENABLED"
            }
        },
        overrides: {
            containerOverrides: [
                {
                    name: process.env.AWS_ECS_CONTAINER_NAME as string,
                    environment: [
                        { name: 'AWS_S3_TEMP_BUCKET', value: bucketName },
                        { name: 'AWS_S3_PROD_BUCKET', value: process.env.AWS_S3_PROD_BUCKET as string },
                        { name: 'AWS_S3_TEMP_KEY', value: objectKey },
                    ],
                }
            ]
        }
    });

    await ecsClient.send(command);
}

async function processMessage(message: any): Promise<void> {
    const messageId = message.MessageId;
    console.log(`Processing message: ${messageId}`);

    try {
        if (!message.Body) {
            throw new Error('Empty message body');
        }

        const event = await validateAndParseEvent(message.Body);

        for (const record of event.Records) {
            const {
                s3: {
                    bucket: { name: bucketName },
                    object: { key: objectKey }
                }
            } = record;

            // Validate S3 key format
            if (!await validateS3Key(objectKey)) {
                console.warn(`Invalid S3 key format: ${objectKey}, skipping...`);
                continue;
            }

            console.log(`Launching ECS task for ${bucketName}/${objectKey}`);
            await retryWithBackoff(() => launchEcsTask(bucketName, objectKey));
        }

        // Delete message after successful processing
        await sqsClient.send(new DeleteMessageCommand({
            QueueUrl: process.env.AWS_SQS_URL as string,
            ReceiptHandle: message.ReceiptHandle
        }));

        console.log(`Successfully processed message: ${messageId}`);
    } catch (error) {
        console.error(`Error processing message ${messageId}:`, error);

        // Extend message visibility timeout on error
        try {
            await sqsClient.send(new ChangeMessageVisibilityCommand({
                QueueUrl: process.env.AWS_SQS_URL as string,
                ReceiptHandle: message.ReceiptHandle,
                VisibilityTimeout: VISIBILITY_TIMEOUT
            }));
        } catch (visibilityError) {
            console.error('Failed to extend message visibility:', visibilityError);
        }

        throw error;
    }
}

// Main loop
async function init() {
    validateEnvironment();

    console.log('Starting SQS message processor...');

    const receiveCommand = new ReceiveMessageCommand({
        QueueUrl: process.env.AWS_SQS_URL as string,
        MaxNumberOfMessages: BATCH_SIZE,
        WaitTimeSeconds: WAIT_TIME_SECONDS
    });

    while (true) {
        try {
            const { Messages } = await sqsClient.send(receiveCommand);

            console.log(Messages);

            if (!Messages || Messages.length === 0) {
                console.log('No messages in Queue');
                continue;
            }

            console.log(`Received ${Messages.length} messages`);

            // Process messages in parallel with error handling for each
            await Promise.all(
                Messages.map(message =>
                    processMessage(message).catch(error => {
                        console.error(`Failed to process message ${message.MessageId}:`, error);
                    })
                )
            );
        } catch (error) {
            console.error('Error in message processing loop:', error);
            // Add small delay before retrying to prevent tight loop on persistent errors
            await sleep(1000);
        }
    }
}

// Handle graceful shutdown
process.on('SIGTERM', () => {
    console.log('Received SIGTERM signal. Shutting down gracefully...');
    // Allow some time for current operations to complete
    setTimeout(() => {
        console.log('Shutdown complete');
        process.exit(0);
    }, 5000);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    // Exit with error to ensure process manager restarts the service
    process.exit(1);
});

init().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});