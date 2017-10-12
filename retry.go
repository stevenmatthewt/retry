package retry

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
)

// MaxQueueDelaySeconds is the maximum number of seconds that a message
// can be delayed in the queue. SQS currently allows up to 900
const MaxQueueDelaySeconds = 2

// Config defines parameters used in the polling process
type Config struct {
	QueueURL        string
	AWSSecret       string
	AWSAccessKeyID  string
	AWSRegion       string
	BackoffStrategy BackoffFunc
	ErrorHandler    ErrorHandler
	Handler         ActionHandler
}

type ActionHandler func(Message) (complete bool)

// BackoffFunc is a function that maps the retry attempt
// to a delay (in seconds)
type BackoffFunc func(attempt uint) (delay uint)

type ErrorHandler func(error)

type message struct {
	Message
	AdditionalDelay uint `json:"accumulated_delay"`
}

type Message struct {
	ID             int  `json:"id"`
	AttemptedCount uint `json:"attempted_count"`
}

type builder struct {
	config Config
}

// New begins polling based on the provided Config
func New(config Config) builder {
	b := builder{
		config: config,
	}
	go b.poll()
	return b
}

func (b builder) Job(id int) error {
	// Probably execute the job immediately if backoff is 0
	// Will prevent unecessary additions to the queue

	return b.sendToQueue(message{
		Message: Message{
			ID:             id,
			AttemptedCount: 0,
		},
		AdditionalDelay: 0,
	}, b.config.BackoffStrategy(0))
}

func (b builder) sendToQueue(message message, delay uint) error {
	body, err := json.Marshal(message)
	if err != nil {
		return errors.Wrap(err, "failed to convert Message to JSON")
	}

	input := &sqs.SendMessageInput{
		MessageBody:  aws.String(string(body)),
		QueueUrl:     aws.String(b.config.QueueURL),
		DelaySeconds: aws.Int64(int64(delay)),
	}

	sess := session.New(&aws.Config{
		Region:      aws.String(b.config.AWSRegion),
		Credentials: credentials.NewStaticCredentials(b.config.AWSAccessKeyID, b.config.AWSSecret, ""),
	})

	svc := sqs.New(sess)
	_, err = svc.SendMessage(input)
	if err != nil {
		return errors.Wrap(err, "failed to send job to SQS")
	}

	return nil
}

func (b builder) poll() {
	sess := session.New(&aws.Config{
		Region:      aws.String(b.config.AWSRegion),
		Credentials: credentials.NewStaticCredentials(b.config.AWSAccessKeyID, b.config.AWSSecret, ""),
	})
	for {
		svc := sqs.New(sess)
		params := &sqs.ReceiveMessageInput{
			QueueUrl:        aws.String(b.config.QueueURL),
			WaitTimeSeconds: aws.Int64(10),
		}
		output, err := svc.ReceiveMessage(params)
		if err != nil {
			err = errors.Wrap(err, "failed to retrieve SQS message")
			b.config.ErrorHandler(err)
			// TODO: do something with the error
		}
		if len(output.Messages) != 1 {
			continue
		}

		sqsMessage := output.Messages[0]
		if _, err := b.deleteMessage(sqsMessage); err != nil {
			// HANDLE ERROR log.WithField("error", err).Errorf("deleting sqs message: %q", *sqsMessage.MessageId)
			err = errors.Wrap(err, "failed to delete SQS message")
			b.config.ErrorHandler(err)
			return
		}

		if sqsMessage.Body == nil {
			continue
		}
		var message message
		err = json.Unmarshal([]byte(*sqsMessage.Body), &message)
		if err != nil {
			err = errors.Wrap(err, "failed to read SQS message as JSON")
			b.config.ErrorHandler(err)
			// TODO: do something with the error
		}

		fmt.Printf("message: %+v\n", message)

		// Compute visiblity timeout and update message to account for backoff
		message, delay, skip := b.computeMessageDelay(message)
		fmt.Printf("message: %+v, delay: %d, skip: %v\n", message, delay, skip)

		if !skip {
			// Perform the action requested for this item
			// TODO: this could theoretically take a very long time.
			// That would cause a cumulative timing error to build up,
			// and eventually we would not be even close to on schedule.
			// We might need to switch to timestamps to keep track.
			complete := b.config.Handler(message.Message)
			if complete {
				continue
			}
		}
		b.sendToQueue(message, delay)
	}
}

func (b builder) deleteMessage(message *sqs.Message) (*sqs.DeleteMessageOutput, error) {
	sess := session.New(&aws.Config{
		Region:      aws.String(b.config.AWSRegion),
		Credentials: credentials.NewStaticCredentials(b.config.AWSAccessKeyID, b.config.AWSSecret, ""),
	})

	svc := sqs.New(sess)
	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(b.config.QueueURL),
		ReceiptHandle: message.ReceiptHandle,
	}
	return svc.DeleteMessage(params)
}

func (b builder) computeMessageDelay(message message) (message, uint, bool) {
	// If the item needs to be delayed more, even though we
	// aren't yet to the next backoff iteration.
	// We need this check since we are limited in how much
	// we can delay messages in SQS
	if message.AdditionalDelay != 0 {
		var delay uint
		if message.AdditionalDelay > MaxQueueDelaySeconds {
			message.AdditionalDelay -= MaxQueueDelaySeconds
			delay = MaxQueueDelaySeconds
		} else {
			delay = message.AdditionalDelay
			message.AdditionalDelay = 0
		}
		return message, delay, true
	}

	// We are actually proceeding to the next iteration of backoff...
	message.AttemptedCount++
	delay := b.config.BackoffStrategy(message.AttemptedCount)
	if delay > MaxQueueDelaySeconds {
		message.AdditionalDelay = delay - MaxQueueDelaySeconds
		delay = MaxQueueDelaySeconds
	}
	return message, delay, false
}

// ExponentialBackoff makes an immediate attempt
// and then backs off exponentially. I.e:
//
// For a seed delay of 5 seconds:
// Attempt 0 - delay 0 seconds
// Attempt 1 - delay 5 seconds
// Attempt 2 - delay 10 seconds
// Attempt 3 - delay 20 seconds
func ExponentialBackoff(seedDelay uint) BackoffFunc {
	return func(attempt uint) uint {
		if attempt == 0 {
			return 0
		}
		return seedDelay << (attempt - 1)
	}
}

// LinearBackoff makes an immediate attempt
// and then backs off linearly. I.e:
//
// For a seed delay of 5 seconds:
// Attempt 0 - delay 0 seconds
// Attempt 1 - delay 5 seconds
// Attempt 2 - delay 10 seconds
// Attempt 3 - delay 15 seconds
func LinearBackoff(seedDelay uint) BackoffFunc {
	return func(attempt uint) uint {
		return seedDelay * attempt
	}
}

// ConstantBackoff makes an immediate attempt
// and then backs off linearly. I.e:
//
// For a seed delay of 5 seconds:
// Attempt 0 - delay 0 seconds
// Attempt 1 - delay 5 seconds
// Attempt 2 - delay 5 seconds
// Attempt 3 - delay 5 seconds
func ConstantBackoff(seedDelay uint) BackoffFunc {
	return func(attempt uint) uint {
		return seedDelay
	}
}
