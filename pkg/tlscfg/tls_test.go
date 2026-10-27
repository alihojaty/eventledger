package tlscfg

import "testing"

func TestMTLS_CredentialsGeneration(t *testing.T) {
	srvCreds, err := ServerTransportCredentials("", "", "")
	if err != nil {
		t.Fatalf("expected server mTLS credentials to succeed, got: %v", err)
	}
	if srvCreds.Info().SecurityProtocol != "tls" {
		t.Errorf("expected security protocol 'tls', got %q", srvCreds.Info().SecurityProtocol)
	}

	cliCreds, err := ClientTransportCredentials("", "", "")
	if err != nil {
		t.Fatalf("expected client mTLS credentials to succeed, got: %v", err)
	}
	if cliCreds.Info().SecurityProtocol != "tls" {
		t.Errorf("expected security protocol 'tls', got %q", cliCreds.Info().SecurityProtocol)
	}
}
