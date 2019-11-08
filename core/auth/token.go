package auth

import (
	"io/ioutil"
	"net/http"
	"strings"
)

type AuthenticationTokenSupplier func() []byte

func NewAuthenticationTokenFromSupplier(tokenSupplier AuthenticationTokenSupplier) Authentication {
	return authenticationToken{supplier: tokenSupplier}
}
func NewAuthenticationTokenFromString(token string) Authentication {
	token = strings.TrimPrefix(token, "token:")
	return authenticationToken{supplier: func() []byte {
		return []byte(token)
	}}
}
func NewAuthenticationTokenFromFile(fileName string) Authentication {
	fileName = strings.TrimPrefix(fileName, "file:")
	return authenticationToken{supplier: func() []byte {
		if content, err := ioutil.ReadFile(fileName); err == nil {
			return content
		} else {
			panic(err)
		}
	}}
}

type authenticationToken struct {
	supplier AuthenticationTokenSupplier
}

func (a authenticationToken) GetAuthMethodName() string {
	return "token"
}
func (a authenticationToken) GetAuthData() AuthenticationDataProvider {
	return authenticationDataToken{supplier: a.supplier}
}

type authenticationDataToken struct {
	authenticationDataNull
	supplier AuthenticationTokenSupplier
}

func (adToken authenticationDataToken) HasDataForHttp() bool {
	return true
}
func (adToken authenticationDataToken) GetHttpHeaders() http.Header {
	return http.Header{
		"Authorization": []string{"Bearer " + string(adToken.supplier())},
	}
}

func (adToken authenticationDataToken) HasDataFromCommand() bool {
	return true
}
func (adToken authenticationDataToken) GetCommandData() []byte {
	return adToken.supplier()
}
