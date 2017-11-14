package retry

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
)

// MaxQueueDelaySeconds is the maximum number of seconds that a message
// can be delayed in the queue. SQS currently allows up to 900
const MaxQueueDelaySeconds = 900

// Config defines parameters used in the polling process
type Config struct {
	QueueURL string
	// AWS* environment variables are optional.
	// If they are not provided, default AWS credential
	// providers will be used.
	AWSSecret       string
	AWSAccessKeyID  string
	AWSRegion       string
	MaxAttempts     int
	BackoffStrategy BackoffFunc
	ErrorHandler    ErrorHandler
	Handler         ActionHandler
}

type ActionHandler func(Message) (complete bool)

type ErrorHandler func(error)

type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

type message struct {
	Message
	ReceivedTime time.Time `json:"received_time"`
	NextAttempt  time.Time `json:"next_attempt"`
}

type Message struct {
	ID             int  `json:"id"`
	AttemptedCount uint `json:"attempted_count"`
}

type Retrier struct {
	time   clock
	config Config
	sqs    sqsiface.SQSAPI
}

// New begins polling based on the provided Config
func New(config Config) Retrier {
	var creds *credentials.Credentials
	if config.AWSAccessKeyID != "" || config.AWSSecret != "" {
		creds = credentials.NewStaticCredentials(config.AWSAccessKeyID, config.AWSSecret, "")
	}
	var region *string
	if config.AWSRegion != "" {
		region = aws.String(config.AWSRegion)
	}

	b := Retrier{
		time:   realClock{},
		config: config,
		sqs: sqs.New(session.New(&aws.Config{
			Region:      region,
			Credentials: creds,
		})),
	}
	go b.poll()
	return b
}

func (r Retrier) Job(id int) error {
	message := message{
		Message: Message{
			ID:             id,
			AttemptedCount: 0,
		},
		ReceivedTime: r.time.Now(),
		NextAttempt:  r.time.Now().Add(r.config.BackoffStrategy(0)),
	}

	return r.workMessage(message)
}

// workMessage handles a message after we've taken it out of SQS
// (or we create it manually if it's a new job)
func (r Retrier) workMessage(message message) error {
	// Compute visiblity timeout and update message to account for backoff
	message, skip := r.computeMessageDelay(message)
	if !skip {
		// Perform the action requested for this item
		complete := r.config.Handler(message.Message)
		if complete {
			return nil
		}
	}
	return r.sendToQueue(message)
}

func (r Retrier) sendToQueue(message message) error {
	body, err := json.Marshal(message)
	if err != nil {
		return errors.Wrap(err, "failed to convert Message to JSON")
	}

	delay := message.NextAttempt.Sub(r.time.Now())

	input := &sqs.SendMessageInput{
		MessageBody:  aws.String(string(body)),
		QueueUrl:     aws.String(r.config.QueueURL),
		DelaySeconds: aws.Int64(int64(delay.Seconds())),
	}

	_, err = r.sqs.SendMessage(input)
	if err != nil {
		return errors.Wrap(err, "failed to send job to SQS")
	}

	return nil
}

func (r Retrier) poll() {
	for {
		r.pollOnce()
	}
}

func (r Retrier) pollOnce() {
	params := &sqs.ReceiveMessageInput{
		QueueUrl:        aws.String(r.config.QueueURL),
		WaitTimeSeconds: aws.Int64(10),
	}
	output, err := r.sqs.ReceiveMessage(params)
	if err != nil {
		err = errors.Wrap(err, "failed to retrieve SQS message")
		r.config.ErrorHandler(err)
		return
	}
	if len(output.Messages) != 1 {
		return
	}

	sqsMessage := output.Messages[0]
	if sqsMessage.Body == nil {
		r.config.ErrorHandler(errors.New("The message retreived from SQS has no body"))
		return
	}
	var message message
	err = json.Unmarshal([]byte(*sqsMessage.Body), &message)
	if err != nil {
		err = errors.Wrap(err, "failed to read SQS message as JSON")
		r.config.ErrorHandler(err)
		return
	}

	if r.config.MaxAttempts != 0 && int(message.AttemptedCount) >= r.config.MaxAttempts {
		// We're just not going to process it which will put it in the DLQ
		// Maybe just calling the ErrorHandler is better though?
		// If someone doesn't have a DLQ set up, then these messages will exist forever
		return
	}

	err = r.workMessage(message)
	if err != nil {
		r.config.ErrorHandler(err)
	}

	// Delete the SQS message
	// Any return that happens prior to this point will put the message in DLQ
	if _, err := r.deleteMessage(sqsMessage); err != nil {
		err = errors.Wrap(err, "failed to delete SQS message")
		r.config.ErrorHandler(err)
		return
	}
}

func (r Retrier) deleteMessage(message *sqs.Message) (*sqs.DeleteMessageOutput, error) {

	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.config.QueueURL),
		ReceiptHandle: message.ReceiptHandle,
	}
	return r.sqs.DeleteMessage(params)
}

func (r Retrier) computeMessageDelay(message message) (message, bool) {
	// If the item needs to be delayed more, even though we
	// aren't yet to the next backoff iteration.
	// We need this check since we are limited in how much
	// we can delay messages in SQS
	//
	// We set our threshold at 5 seconds, not zero.
	// This prevent extraneous reads from the queue due
	// to SQS Timing inaccuracies.
	var additionalDelay = message.NextAttempt.Sub(r.time.Now())
	if additionalDelay.Seconds() > 5 {
		return message, true
	}

	// We are actually proceeding to the next iteration of backoff...
	message.AttemptedCount++
	delay := r.config.BackoffStrategy(message.AttemptedCount)
	message.NextAttempt = message.NextAttempt.Add(time.Duration(delay) * time.Second)
	return message, false
}
