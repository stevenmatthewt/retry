package retry

import (
	"testing"
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
		},
	}

	for _, test := range cases {
		mockFunc := &mockFunc{
			SucceedOnAttemptNumber: test.succeedOnAttemptNumber,
		}
		mockSQS := NewMockSQS()
		retrier := Retrier{
			config: Config{
				AWSAccessKeyID:  "",
				AWSSecret:       "",
				AWSRegion:       "",
				QueueURL:        "",
				BackoffStrategy: LinearBackoff(4),
				MaxAttempts:     test.maxAttempts,
				ErrorHandler: func(err error) {
					t.Error(err)
				},
				Handler: mockFunc.invoke,
			},
			sqs: mockSQS,
		}

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

		if t.Failed() {
			t.Logf("Failed test: %s", test.description)
		}
	}
}
