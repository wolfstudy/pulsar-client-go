package auth

import (
	"crypto/tls"
	"net/http"
)

type (
	Authentication interface {
		GetAuthMethodName() string
		GetAuthData() AuthenticationDataProvider
	}

	AuthenticationDataProvider interface {
		HasDataForTls() bool
		GetTlsCertificates() []tls.Certificate
		// GetTslPrivateKey is redundant due to Go TLS implementation

		HasDataForHttp() bool
		GetHttpAuthType() string
		GetHttpHeaders() http.Header

		HasDataFromCommand() bool
		GetCommandData() []byte
	}
)
