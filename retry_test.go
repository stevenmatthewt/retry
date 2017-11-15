package retry

import (
	"testing"
	"time"
)

type testcase struct {
	description               string
	numberOfPolls             int
	succeedOnAttemptNumber    int
	maxAttempts               int
	expectHandlerInvokedCount int
	expectSQSReceiveCount     int
	expectSQSSendCount        int
	expectSQSDeleteCount      int
	backoff                   BackoffFunc
	elapsedTime               time.Duration
}

func TestRetry(t *testing.T) {
	cases := []testcase{
		testcase{
			description:               "execute job immediately",
			numberOfPolls:             0,
			succeedOnAttemptNumber:    0,
			maxAttempts:               1,
			expectHandlerInvokedCount: 1,
			expectSQSReceiveCount:     0,
			expectSQSSendCount:        0,
			expectSQSDeleteCount:      0,
			backoff:                   LinearBackoff(time.Second * 4),
			elapsedTime:               time.Second * 0,
		},
		testcase{
			description:               "execute job after one delay",
			numberOfPolls:             1,
			succeedOnAttemptNumber:    2,
			maxAttempts:               2,
			expectHandlerInvokedCount: 2,
			expectSQSReceiveCount:     1,
			expectSQSSendCount:        1,
			expectSQSDeleteCount:      1,
			backoff:                   LinearBackoff(time.Second * 10),
			elapsedTime:               time.Second * 10,
		},
		testcase{
			description:               "execute job after many delays",
			numberOfPolls:             12,
			succeedOnAttemptNumber:    13,
			maxAttempts:               13,
			expectHandlerInvokedCount: 13,
			expectSQSReceiveCount:     12,
			expectSQSSendCount:        12,
			expectSQSDeleteCount:      12,
			backoff:                   LinearBackoff(time.Second * 4),
			elapsedTime:               time.Minute*5 + time.Second*12,
		},
		testcase{
			description:               "send job to DLQ",
			numberOfPolls:             4,
			succeedOnAttemptNumber:    99,
			maxAttempts:               4,
			expectHandlerInvokedCount: 4,
			expectSQSReceiveCount:     4,
			expectSQSSendCount:        4,
			expectSQSDeleteCount:      3,
			backoff:                   LinearBackoff(time.Second * 4),
			elapsedTime:               time.Second * 40,
		},
		testcase{
			description:               "Delay for greater than maximum",
			numberOfPolls:             2,
			succeedOnAttemptNumber:    2,
			maxAttempts:               2,
			expectHandlerInvokedCount: 2,
			expectSQSReceiveCount:     2,
			expectSQSSendCount:        2,
			expectSQSDeleteCount:      2,
			backoff:                   LinearBackoff(time.Second * 1000),
			elapsedTime:               time.Second * 1000,
		},
		testcase{
			description:               "Delay for one hour",
			numberOfPolls:             4,
			succeedOnAttemptNumber:    2,
			maxAttempts:               2,
			expectHandlerInvokedCount: 2,
			expectSQSReceiveCount:     4,
			expectSQSSendCount:        4,
			expectSQSDeleteCount:      4,
			backoff: func(attempt uint) time.Duration {
				if attempt == 0 {
					return 0
				}
				return time.Hour
			},
			elapsedTime: time.Hour,
		},
	}

	for _, test := range cases {
		mockFunc := &mockFunc{
			SucceedOnAttemptNumber: test.succeedOnAttemptNumber,
		}
		clock := &mockClock{}
		mockSQS := NewMockSQS(clock)
		retrier := Retrier{
			config: Config{
				AWSAccessKeyID:  "",
				AWSSecret:       "",
				AWSRegion:       "",
				QueueURL:        "",
				BackoffStrategy: test.backoff,
				MaxAttempts:     test.maxAttempts,
				ErrorHandler: func(err error) {
					t.Error(err)
				},
				Handler: mockFunc.invoke,
			},
			sqs:  mockSQS,
			time: clock,
		}

		startTime := mockSQS.clock.Now()
		err := retrier.Job(0)
		if err != nil {
			t.Error(err)
		}

		for i := 0; i < test.numberOfPolls; i++ {
			retrier.pollOnce()
		}

		if got, want := mockFunc.InvokedCount, test.expectHandlerInvokedCount; got != want {
			t.Errorf("mock function invoked incorrect number of times. expected=%d actual=%d", want, got)
		}
		if got, want := mockSQS.ReceiveMessageInvokedCount, test.expectSQSReceiveCount; got != want {
			t.Errorf("SQS ReceiveMessage invoked incorrect number of times, expected=%d actual=%d", want, got)
		}
		if got, want := mockSQS.SendMessageInvokedCount, test.expectSQSSendCount; got != want {
			t.Errorf("SQS SendMessage invoked incorrect number of times, expected=%d actual=%d", want, got)
		}
		if got, want := mockSQS.DeleteMessageInvokedCount, test.expectSQSDeleteCount; got != want {
			t.Errorf("SQS DeleteMessage invoked incorrect number of times, expected=%d actual=%d", want, got)
		}
		if got, want := mockSQS.clock.Now().Sub(startTime), test.elapsedTime; got != want {
			t.Errorf("Incorrect time elapsed, expected=%v actual=%v", want, got)
		}

		if t.Failed() {
			t.Logf("Failed test: %s", test.description)
		}
	}
}
