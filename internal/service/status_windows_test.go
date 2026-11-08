package service

import "testing"

// Run as a normal user: a running service must not read as "unknown".
func TestStatusWithoutAdmin(t *testing.T) {
	if got := statusOf("EventLog"); got != "running" {
		t.Fatalf("EventLog: %s", got)
	}
	if got := statusOf("kita-connector-no-such-service"); got != "not installed" {
		t.Fatalf("missing service: %s", got)
	}
}
