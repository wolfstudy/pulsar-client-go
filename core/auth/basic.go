package auth

import (
	"encoding/base64"
	"net/http"
)

func NewAuthenticationBasic(userId, password string) Authentication {
	return authenticationBasic{userId: userId, password: password}
}

type authenticationBasic struct {
	userId, password string
}

func (a authenticationBasic) GetAuthMethodName() string {
	return "basic"
}
func (a authenticationBasic) GetAuthData() AuthenticationDataProvider {
	commandAuthToken := []byte(a.userId + ":" + a.password)
	httpAuthToken := "Basic " + base64.StdEncoding.EncodeToString(commandAuthToken)
	return authenticationDataBasic{
		httpAuthToken:    httpAuthToken,
		commandAuthToken: commandAuthToken,
	}
}

type authenticationDataBasic struct {
	authenticationDataNull
	httpAuthToken    string
	commandAuthToken []byte
}

func (adBasic authenticationDataBasic) HasDataForHttp() bool {
	return true
}
func (adBasic authenticationDataBasic) GetHttpHeaders() http.Header {
	return http.Header{
		"Authorization": []string{adBasic.httpAuthToken},
	}
}

func (adBasic authenticationDataBasic) HasDataFromCommand() bool {
	return true
}
func (adBasic authenticationDataBasic) GetCommandData() []byte {
	return adBasic.commandAuthToken
}
