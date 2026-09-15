package provider

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// metadataIPs are infrastructure endpoints that the generic range checks do
// not cover but that must never be provider destinations. The cloud metadata
// services on 169.254.169.254 (AWS/GCP/Azure, IPv4) and fd00:ec2::254 (AWS,
// IPv6) already fall in the link-local and unique-local ranges; Alibaba's
// endpoint sits in shared CGNAT space and needs this explicit entry.
var metadataIPs = map[string]struct{}{
	"100.100.100.200": {},
}

// ValidateBaseURL enforces SSRF protections on provider base URLs. URLs come
// only from trusted configuration, but misconfiguration must fail loudly:
// only http(s) schemes without userinfo are accepted, plain http is rejected
// unless explicitly allowed for local development, and IP-literal hosts must
// not target loopback, private, link-local (cloud metadata), multicast, or
// unspecified addresses. Development mode (allowInsecureHTTP) narrowly
// re-allows loopback and private IP literals so local model servers and
// container networks stay reachable; the always-unsafe classes remain
// rejected in every mode, and clients can never supply provider URLs.
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
	if ip := net.ParseIP(host); ip != nil {
		if allowInsecureHTTP && localDevIP(ip) {
			return nil // explicit development allowance: loopback/private only
		}
		if restrictedProviderIP(ip) {
			return fmt.Errorf("base url host %q targets a restricted IP destination (loopback, private, link-local, multicast, unspecified, or metadata service)", host)
		}
		return nil // public IP literals are trusted operator configuration
	}
	return nil
}

// restrictedProviderIP reports whether the IP targets infrastructure a
// provider base URL must never reference: loopback, private (RFC 1918 and
// unique-local), link-local unicast (including cloud metadata services),
// link-local multicast, multicast, unspecified, or an explicitly listed
// metadata endpoint.
func restrictedProviderIP(ip net.IP) bool {
	if _, ok := metadataIPs[ip.String()]; ok {
		return true
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified()
}

// localDevIP reports whether the IP is a loopback or private destination,
// the narrow class allowed only under explicit local-development
// configuration.
func localDevIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate()
}
