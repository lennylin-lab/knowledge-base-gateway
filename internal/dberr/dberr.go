// Package dberr classifies PostgreSQL connection failures for logging without
// echoing any DSN component. pgconn wraps connection strings in its parse and
// connect errors (host, user, database, and the password-redacted URL all
// appear in the message text), so errors are inspected and reduced to a
// coarse operation-plus-cause phrase. Keyword matching runs against the
// lowercased message only; the message itself is never returned.
package dberr

import (
	"context"
	"errors"
	"strings"
)

// DescribeConnectFailure maps a database connection error to a short,
// secret-free classification suitable for logs and CLI output.
func DescribeConnectFailure(err error) string {
	if err == nil {
		return "unknown failure"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timed out reaching the server"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "password authentication failed"),
		strings.Contains(msg, "authentication failed"),
		strings.Contains(msg, "login"),
		strings.Contains(msg, `role "`),
		strings.Contains(msg, "tenant or user not found"):
		return "the server rejected the credentials"
	case strings.Contains(msg, "no such host"),
		strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "host unreachable"),
		strings.Contains(msg, "hostname resolving"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "dial error"):
		return "the server was unreachable (dns, dial, or timeout failure)"
	case strings.Contains(msg, "parse"),
		strings.Contains(msg, "invalid"),
		strings.Contains(msg, "keyword"),
		strings.Contains(msg, "unsupported"):
		return "the configured connection string is invalid"
	default:
		return "a connection could not be established"
	}
}
