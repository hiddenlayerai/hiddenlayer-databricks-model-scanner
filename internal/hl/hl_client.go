package hl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// Auth authenticates with the HiddenLayer API and returns an access token.
func Auth(apiId string, apiKey string) (string, error) {
	transport := &http.Transport{
		// Set the maximum number of idle connections
		MaxIdleConns: 10,
		// Set the maximum number of idle connections per host
		MaxIdleConnsPerHost: 10,
		// Set the idle connection timeout
		IdleConnTimeout: 30 * time.Second,
	}

	// Create an HTTP client with the custom transport
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Minute,
	}

	accessToken, err := GetJwt(httpClient, apiId, apiKey)
	if err != nil {
		return "", err
	}
	return accessToken, nil
}

// GetJwt authenticates with the HiddenLayer API and returns a JWT token.
func GetJwt(httpClient *http.Client, apiId string, apiKey string) (string, error) {
	// Hardwire the production URL for now.
	// (The AI PC demo knows how to use staging also, we could add that later.)
	const authUrl = "https://auth.hiddenlayer.ai"
	url := fmt.Sprintf("%s/oauth2/token?grant_type=client_credentials", authUrl)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return "", err
	}

	req.SetBasicAuth(apiId, apiKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer CloseBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(
			fmt.Sprintf("Unable to get authentication credentials for the HiddenLayer API: %d: %s",
				resp.StatusCode, resp.Status))
	}

	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return "", err
	}

	accessToken, ok := result["access_token"].(string)
	if !ok {
		return "", errors.New(
			"unable to get authentication credentials for the HiddenLayer API - invalid response")
	}

	return accessToken, nil
}

// CloseBody closes the io.ReadCloser. If there is an error, it logs the error and exits the program.
// Should never happen.
func CloseBody(body io.ReadCloser) {
	err := body.Close()
	if err != nil {
		log.Fatalf("Error closing response body: %v", err)
	}
}
