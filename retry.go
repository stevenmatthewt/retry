package retry

import (
	"encoding/json"
	"fmt"
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
	QueueURL        string
	AWSSecret       string
	AWSAccessKeyID  string
	AWSRegion       string
	MaxAttempts     int
	BackoffStrategy BackoffFunc
	ErrorHandler    ErrorHandler
	Handler         ActionHandler
}

type ActionHandler func(Message) (complete bool)

// BackoffFunc is a function that maps the retry attempt
// to a delay (in seconds)
type BackoffFunc func(attempt uint) (delay uint)

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
	b := Retrier{
		time:   realClock{},
		config: config,
		sqs: sqs.New(session.New(&aws.Config{
			Region:      aws.String(config.AWSRegion),
			Credentials: credentials.NewStaticCredentials(config.AWSAccessKeyID, config.AWSSecret, ""),
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
		NextAttempt:  r.time.Now(),
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

	fmt.Println("got message")
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
		fmt.Println("abourting")
		return
	}

	fmt.Println("processing message")

	// Delete the SQS message
	// Any error that happens prior to this point will put the message in DLQ
	// Any error after it will not.
	if _, err := r.deleteMessage(sqsMessage); err != nil {
		err = errors.Wrap(err, "failed to delete SQS message")
		r.config.ErrorHandler(err)
		return
	}

	fmt.Printf("message: %+v\n", message)
	err = r.workMessage(message)
	if err != nil {
		r.config.ErrorHandler(err)
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
	fmt.Printf("compute message: %+v current time: %v\n", message, r.time.Now())
	var additionalDelay = message.NextAttempt.Sub(r.time.Now())
	if additionalDelay.Seconds() > 0 {
		fmt.Print("skipping\n")
		return message, true
	}

	// We are actually proceeding to the next iteration of backoff...
	message.AttemptedCount++
	delay := r.config.BackoffStrategy(message.AttemptedCount)
	message.NextAttempt = message.NextAttempt.Add(time.Duration(delay) * time.Second)
	return message, false
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
