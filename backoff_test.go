package retry

import (
	"testing"
	"time"
)

type backoffCase struct {
	seedDelay time.Duration
	attempt   uint
	expect    time.Duration
}

func TestExponential(t *testing.T) {
	cases := []backoffCase{
		// attempt 0 should always yield 0
		backoffCase{
			seedDelay: time.Second,
			attempt:   0,
			expect:    time.Second * 0,
		},
		backoffCase{
			seedDelay: time.Minute,
			attempt:   0,
			expect:    time.Second * 0,
		},
		backoffCase{
			seedDelay: time.Hour*6 + time.Minute*9,
			attempt:   0,
			expect:    time.Second * 0,
		},
		// varying attempts produce varying results
		backoffCase{
			seedDelay: time.Second,
			attempt:   1,
			expect:    time.Second,
		},
		backoffCase{
			seedDelay: time.Minute * 3,
			attempt:   2,
			expect:    time.Minute * 6,
		},
		backoffCase{
			seedDelay: time.Minute * 3,
			attempt:   3,
			expect:    time.Minute * 12,
		},
		// crazy
		backoffCase{
			seedDelay: time.Hour*7 + time.Minute*3 + time.Second*33,
			attempt:   9,
			expect:    time.Hour*1807 + time.Minute*8 + time.Second*48,
		},
	}

	for _, test := range cases {
		if got, want := ExponentialBackoff(test.seedDelay)(test.attempt), test.expect; got != want {
			t.Errorf("received incorrect backoff delay got=%s want=%s", got, want)
		}
	}
}

func TestLinear(t *testing.T) {
	cases := []backoffCase{
		// attempt 0 should always yield 0
		backoffCase{
			seedDelay: time.Second,
			attempt:   0,
			expect:    time.Second * 0,
		},
		backoffCase{
			seedDelay: time.Minute,
			attempt:   0,
			expect:    time.Second * 0,
		},
		backoffCase{
			seedDelay: time.Hour*6 + time.Minute*9,
			attempt:   0,
			expect:    time.Second * 0,
		},
		// varying attempts produce varying results
		backoffCase{
			seedDelay: time.Second,
			attempt:   1,
			expect:    time.Second,
		},
		backoffCase{
			seedDelay: time.Minute * 3,
			attempt:   2,
			expect:    time.Minute * 6,
		},
		backoffCase{
			seedDelay: time.Minute * 3,
			attempt:   3,
			expect:    time.Minute * 9,
		},
		// crazy
		backoffCase{
			seedDelay: time.Hour*7 + time.Minute*3 + time.Second*33,
			attempt:   9,
			expect:    time.Hour*63 + time.Minute*31 + time.Second*57,
		},
	}

	for _, test := range cases {
		if got, want := LinearBackoff(test.seedDelay)(test.attempt), test.expect; got != want {
			t.Errorf("received incorrect backoff delay got=%s want=%s", got, want)
		}
	}
}

func TestConstant(t *testing.T) {
	cases := []backoffCase{
		// attempt 0 should always yield 0
		backoffCase{
			seedDelay: time.Second,
			attempt:   0,
			expect:    time.Second * 0,
		},
		backoffCase{
			seedDelay: time.Minute,
			attempt:   0,
			expect:    time.Second * 0,
		},
		backoffCase{
			seedDelay: time.Hour*6 + time.Minute*9,
			attempt:   0,
			expect:    time.Second * 0,
		},
		// varying attempts produce varying results
		backoffCase{
			seedDelay: time.Second,
			attempt:   1,
			expect:    time.Second,
		},
		backoffCase{
			seedDelay: time.Minute * 3,
			attempt:   2,
			expect:    time.Minute * 3,
		},
		backoffCase{
			seedDelay: time.Minute * 3,
			attempt:   3,
			expect:    time.Minute * 3,
		},
		// crazy
		backoffCase{
			seedDelay: time.Hour*7 + time.Minute*3 + time.Second*33,
			attempt:   9,
			expect:    time.Hour*7 + time.Minute*3 + time.Second*33,
		},
	}

	for _, test := range cases {
		if got, want := ConstantBackoff(test.seedDelay)(test.attempt), test.expect; got != want {
			t.Errorf("received incorrect backoff delay got=%s want=%s", got, want)
		}
	}
}
