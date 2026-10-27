package tlscfg

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"

	"google.golang.org/grpc/credentials"
)

// ServerTransportCredentials returns mTLS 1.3 gRPC server credentials for publisher.
func ServerTransportCredentials(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	if certFile != "" && keyFile != "" && caFile != "" {
		return loadFileCredentials(certFile, keyFile, caFile, true)
	}

	caCert, caPriv, caPool, err := deterministicDevCA()
	if err != nil {
		return nil, err
	}

	srvCert, err := issueLeafCert(caCert, caPriv, "publisher", true)
	if err != nil {
		return nil, err
	}

	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{srvCert},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	return credentials.NewTLS(tlsCfg), nil
}

// ClientTransportCredentials returns mTLS 1.3 gRPC client credentials for gateway.
func ClientTransportCredentials(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	if certFile != "" && keyFile != "" && caFile != "" {
		return loadFileCredentials(certFile, keyFile, caFile, false)
	}

	caCert, caPriv, caPool, err := deterministicDevCA()
	if err != nil {
		return nil, err
	}

	cliCert, err := issueLeafCert(caCert, caPriv, "gateway", false)
	if err != nil {
		return nil, err
	}

	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cliCert},
		RootCAs:      caPool,
	}
	return credentials.NewTLS(tlsCfg), nil
}

func loadFileCredentials(certFile, keyFile, caFile string, isServer bool) (credentials.TransportCredentials, error) {
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load x509 key pair: %w", err)
	}
	caBytes, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA file: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("failed to append CA certificate to pool")
	}

	if isServer {
		return credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{pair},
			ClientCAs:    caPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		}), nil
	}

	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{pair},
		RootCAs:      caPool,
	}), nil
}

func deterministicDevCA() (*x509.Certificate, ed25519.PrivateKey, *x509.CertPool, error) {
	seed := sha256.Sum256([]byte("reliable-deposit-processor-internal-mtls-ca-v1"))
	caPriv := ed25519.NewKeyFromSeed(seed[:])
	caPub := caPriv.Public().(ed25519.PublicKey)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Reliable Deposit Processor"},
			CommonName:   "DepositProcessor-Internal-Root-CA",
		},
		NotBefore:             time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:              time.Date(2035, 1, 1, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(nil, tpl, tpl, caPub, caPriv)
	if err != nil {
		return nil, nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, nil, err
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return caCert, caPriv, pool, nil
}

func issueLeafCert(caCert *x509.Certificate, caPriv ed25519.PrivateKey, cn string, isServer bool) (tls.Certificate, error) {
	leafPub, leafPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return tls.Certificate{}, err
	}

	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{"Reliable Deposit Processor"},
			CommonName:   cn,
		},
		NotBefore: time.Now().Add(-1 * time.Hour),
		NotAfter:  time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:  x509.KeyUsageDigitalSignature,
	}

	if isServer {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tpl.DNSNames = []string{"publisher", "localhost", "bufnet"}
		tpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
	} else {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, tpl, caCert, leafPub, caPriv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	privBytes, err := x509.MarshalPKCS8PrivateKey(leafPriv)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})

	return tls.X509KeyPair(certPEM, keyPEM)
}
