package provider

// URL safety tests for ValidateBaseURL (issue #1 R5/R6, AC5): production
// configuration rejects loopback, private, link-local (cloud metadata),
// multicast, unspecified, and explicit metadata-service IP destinations;
// development mode narrowly re-allows loopback and private IP literals only.
// All addresses are documentation/reserved literals — no real endpoints.

import "testing"

func TestValidateBaseURLRestrictedIPs(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		allowHTTP bool
		wantErr   bool
	}{
		// Production rejects unsafe IPv4 literals.
		{"prod loopback v4", "https://127.0.0.1:8080/v1", false, true},
		{"prod private 10/8", "https://10.0.0.5/v1", false, true},
		{"prod private 172.16/12", "https://172.16.0.9/v1", false, true},
		{"prod private 192.168/16", "https://192.168.1.10/v1", false, true},
		{"prod metadata link-local", "https://169.254.169.254/v1", false, true},
		{"prod metadata cgnat", "https://100.100.100.200/v1", false, true},
		{"prod multicast", "https://224.0.0.1/v1", false, true},
		{"prod unspecified", "https://0.0.0.0/v1", false, true},
		// Production rejects unsafe IPv6 literals.
		{"prod loopback v6", "https://[::1]/v1", false, true},
		{"prod unique-local", "https://[fd00::1]/v1", false, true},
		{"prod link-local v6", "https://[fe80::1]/v1", false, true},
		{"prod metadata v6", "https://[fd00:ec2::254]/v1", false, true},
		{"prod multicast v6", "https://[ff02::1]/v1", false, true},
		{"prod unspecified v6", "https://[::]/v1", false, true},
		{"prod ipv4-mapped loopback", "https://[::ffff:127.0.0.1]/v1", false, true},
		// Production still accepts public destinations.
		{"prod public ip", "https://192.0.2.10/v1", false, false},
		{"prod public name", "https://api.openai.com/v1", false, false},
		{"prod internal name", "https://llm.internal/v1", false, false},
		// Development allowances are explicit and narrow: loopback and
		// private ranges only, any scheme.
		{"dev loopback v4", "http://127.0.0.1:8080/v1", true, false},
		{"dev loopback v6", "https://[::1]:8080", true, false},
		{"dev private 10/8", "http://10.0.0.5:11434/v1", true, false},
		{"dev private 192.168/16", "http://192.168.1.10:8000/v1", true, false},
		{"dev unique-local", "http://[fd00::1]:8000/v1", true, false},
		// Development must not reopen the always-unsafe classes.
		{"dev metadata link-local", "http://169.254.169.254/v1", true, true},
		{"dev metadata cgnat", "http://100.100.100.200/v1", true, true},
		{"dev multicast", "http://224.0.0.1/v1", true, true},
		{"dev unspecified", "http://0.0.0.0:8080/v1", true, true},
		{"dev link-local v6", "http://[fe80::1]/v1", true, true},
	}
	for _, c := range cases {
		err := ValidateBaseURL(c.raw, c.allowHTTP)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: ValidateBaseURL(%q, %v) err=%v, wantErr=%v", c.name, c.raw, c.allowHTTP, err, c.wantErr)
		}
	}
}
