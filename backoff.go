package retry

import "time"

// BackoffFunc is a function that maps the retry attempt
// to a delay (in seconds)
type BackoffFunc func(attempt uint) (delay time.Duration)

// ExponentialBackoff makes an immediate attempt
// and then backs off exponentially. I.e:
//
// For a seed delay of 5 seconds:
// Attempt 0 - delay 0 seconds
// Attempt 1 - delay 5 seconds
// Attempt 2 - delay 10 seconds
// Attempt 3 - delay 20 seconds
func ExponentialBackoff(seedDelay time.Duration) BackoffFunc {
	return func(attempt uint) time.Duration {
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
func LinearBackoff(seedDelay time.Duration) BackoffFunc {
	return func(attempt uint) time.Duration {
		return seedDelay * time.Duration(attempt)
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
func ConstantBackoff(seedDelay time.Duration) BackoffFunc {
	return func(attempt uint) time.Duration {
		if attempt == 0 {
			return 0
		}
		return seedDelay
	}
}
