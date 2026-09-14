package dberr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

const leakyDSN = "postgres://kb:secret-pw@db-host-internal:5432/kb?sslmode=disable"

func TestDescribeConnectFailureClassifications(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "unknown failure"},
		{"deadline", context.DeadlineExceeded, "timed out reaching the server"},
		{"canceled", context.Canceled, "timed out reaching the server"},
		{"auth", fmt.Errorf("pq: password authentication failed for user %q", "kb"), "the server rejected the credentials"},
		{"role", errors.New(`pq: role "kb" does not exist`), "the server rejected the credentials"},
		{"refused", errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"), "the server was unreachable (dns, dial, or timeout failure)"},
		{"dns", errors.New(`dial tcp: lookup db-host-internal: no such host`), "the server was unreachable (dns, dial, or timeout failure)"},
		{"resolving", errors.New(`failed to connect to ` + "`host=db-host-internal user=kb database=kb`" + `: hostname resolving error`), "the server was unreachable (dns, dial, or timeout failure)"},
		{"parse", fmt.Errorf("parse error on %q", leakyDSN), "the configured connection string is invalid"},
		{"unknown", errors.New("something entirely unexpected"), "a connection could not be established"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DescribeConnectFailure(tc.err); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// No classification may leak any part of the DSN the error text mentions:
// pgconn embeds the password-redacted URL, host, user, and database in its
// messages, so every classification is checked against those substrings.
func TestDescribeConnectFailureNeverEchoesDSN(t *testing.T) {
	leaky := []string{
		leakyDSN,
		"secret-pw",
		"postgres://",
		"db-host-internal",
		"kb:secret-pw",
	}
	errs := []error{
		fmt.Errorf("connect failed: %w", errors.New(leakyDSN)),
		fmt.Errorf("failed to connect to `host=db-host-internal user=kb database=kb`: dial error"),
		errors.New("parse " + leakyDSN + ": invalid keyword"),
	}
	for i, err := range errs {
		got := DescribeConnectFailure(err)
		for _, sub := range leaky {
			if strings.Contains(got, sub) {
				t.Fatalf("case %d: classification %q leaks DSN substring %q", i, got, sub)
			}
		}
	}
}
