# retry

`retry` is a golang package that allows requests to be retried multiple times (on a customizable schedule) until they succeed. It is intended for requests that must have a substantial amount of delay between retries. It is **not** recommended for instances where you need only a few seconds or less of delay.

## SQS
`retry` uses an AWS SQS Queue as a backing mechanism for handling its retries. Every new 'Job' that is requested is added to the queue and is retried after the alloted amount of time has passed. Here's a few requirements of the SQS Queue:

1. The queue must have a `ReceiveMessageWaitTimeSeconds` of 10 seconds.
2. There must be a Dead Letter Queue configured with a redrive policy of exactly 1 Maximum Receives. Any Jobs that are not successfully completed by the configured time are sent to the DLQ.

## Use

```golang
//First, configure and create a new Retrier service:

service := retry.New(Config{
		AWSAccessKeyID:  "<key>",
		AWSSecret:       "<secret>",
		AWSRegion:       "us-east-1",
		QueueURL:        "https://sqs.us-east-1.amazonaws.com/0123456789/example-queue",
		BackoffStrategy: retry.ExponentialBackoff(5),
		MaxAttempts:     4,
		ErrorHandler: func(err error) {
			log.Print(err)
		},
		Handler: func(message retry.Message) bool {
            fmt.Printf("Handling Job %d", message.ID)
            // true means everything was successfull
			return true
		},
    })

// Then each time you have a Job to retry:

service.Job(1)

// The Handler function will be called on the schedule indicated by BackoffStrategy until it returns true. If it does not return true by MaxAttempts, the message will be put in the DLQ
```