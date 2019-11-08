package auth

import (
	"crypto/tls"
	"net/http"
)

func NewAuthenticationDisabled() Authentication {
	return authenticationDisabled{}
}

type authenticationDisabled struct{}

func (a authenticationDisabled) GetAuthMethodName() string {
	return ""
}
func (a authenticationDisabled) GetAuthData() AuthenticationDataProvider {
	return authenticationDataNull{}
}

type authenticationDataNull struct{}

func (adNull authenticationDataNull) HasDataForTls() bool {
	return false
}
func (adNull authenticationDataNull) GetTlsCertificates() []tls.Certificate {
	return nil
}

func (adNull authenticationDataNull) HasDataForHttp() bool {
	return false
}
func (adNull authenticationDataNull) GetHttpAuthType() string {
	return ""
}
func (adNull authenticationDataNull) GetHttpHeaders() http.Header {
	return nil
}

func (adNull authenticationDataNull) HasDataFromCommand() bool {
	return false
}
func (adNull authenticationDataNull) GetCommandData() []byte {
	return nil
}
