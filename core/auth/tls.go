package auth

import "crypto/tls"

func NewAuthenticationTLS(certFile, keyFile string) Authentication {
	return authenticationTls{certFile: certFile, keyFile: keyFile}
}

type authenticationTls struct {
	certFile, keyFile string
}

func (a authenticationTls) GetAuthMethodName() string {
	return "tls"
}
func (a authenticationTls) GetAuthData() AuthenticationDataProvider {
	if certificate, err := tls.LoadX509KeyPair(a.certFile, a.keyFile); err == nil {
		return authenticationDataTls{certificates: []tls.Certificate{certificate}}
	} else {
		panic(err)
	}
}

type authenticationDataTls struct {
	authenticationDataNull
	certificates []tls.Certificate
}

func (adTls authenticationDataTls) HasDataForTls() bool {
	return true
}
func (adTls authenticationDataTls) GetTlsCertificates() []tls.Certificate {
	return adTls.certificates
}
