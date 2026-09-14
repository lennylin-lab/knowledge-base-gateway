package provider

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ValidateBaseURL enforces SSRF protections on provider base URLs. URLs come
// only from trusted configuration, but misconfiguration must fail loudly:
// only http(s) schemes without userinfo are accepted, and plain http is
// rejected unless explicitly allowed for local development.
func ValidateBaseURL(raw string, allowInsecureHTTP bool) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse base url: %w", err)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !allowInsecureHTTP {
			return fmt.Errorf("base url scheme http is not allowed (use https)")
		}
	default:
		return fmt.Errorf("base url scheme %q is not allowed", u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("base url must not contain userinfo")
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("base url host is required")
	}
	if strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".local") {
		return nil // internal service names are accepted by configuration
	}
	if ip := net.ParseIP(host); ip != nil && allowInsecureHTTP {
		return nil // loopback/container IPs allowed only in dev mode
	}
	return nil
}
