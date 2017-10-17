package retry

import (
	"time"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type mockFunc struct {
	SucceedOnAttemptNumber int
	InvokedCount           int
}

func (m *mockFunc) invoke(Message) bool {
	m.InvokedCount++
	return m.InvokedCount >= m.SucceedOnAttemptNumber
}

type mockSQS struct {
	sqsiface.SQSAPI
	storage                    map[*string]*string
	clock                      *mockClock
	ReceiveMessageInvokedCount int
	// ReceiveMessageFunc         func(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	SendMessageInvokedCount int
	// SendMessageFunc            func(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error)
	DeleteMessageInvokedCount int
	// DeleteMessageFunc          func(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
}

func NewMockSQS(clock *mockClock) *mockSQS {
	return &mockSQS{
		storage: make(map[*string]*string),
		clock:   clock,
	}
}

func (m *mockSQS) ReceiveMessage(in *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	m.ReceiveMessageInvokedCount++
	for key, value := range m.storage {
		return &sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{
				&sqs.Message{
					Body:          value,
					ReceiptHandle: key,
				},
			},
		}, nil
	}

	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockSQS) SendMessage(in *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	m.SendMessageInvokedCount++
	str := string(m.SendMessageInvokedCount)
	m.storage[&str] = in.MessageBody

	if in.DelaySeconds != nil {
		m.clock.time = m.clock.time.Add(time.Duration(*in.DelaySeconds) * time.Second)
	}

	return &sqs.SendMessageOutput{}, nil
}

func (m *mockSQS) DeleteMessage(in *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	m.DeleteMessageInvokedCount++
	delete(m.storage, in.ReceiptHandle)
	return &sqs.DeleteMessageOutput{}, nil
}

type mockClock struct {
	time time.Time
}

func (m mockClock) Now() time.Time {
	return m.time
}
