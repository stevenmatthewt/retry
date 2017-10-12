package retry

import (
	"fmt"
	"testing"
	"time"
)

func TestFake(t *testing.T) {
	err := New(Config{
		AWSAccessKeyID:  "REDACTED",
		AWSSecret:       "REDACTED",
		AWSRegion:       "us-east-1",
		QueueURL:        "https://sqs.us-east-1.amazonaws.com/063056737263/assessments-results-polling-test",
		BackoffStrategy: LinearBackoff(4),
		ErrorHandler: func(err error) {
			fmt.Print(err)
		},
		Handler: func(message Message) bool {
			fmt.Printf("%s\n", time.Now())
			return message.AttemptedCount == 4
		},
	}).Job(0)

	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Second * 25)

	t.Fail()
}
