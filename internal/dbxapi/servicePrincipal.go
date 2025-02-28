package dbxapi

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

type ServicePrincipal struct {
	ID   string `json:"id"`
	Name string `json:"displayName"`
}

func ServicePrincipalExists(servicePrincipalName string, dbxHost string, dbxToken string) bool {
	url := fmt.Sprintf("%s/api/2.0/preview/scim/v2/ServicePrincipals", dbxHost)

	method := "GET"

	client := &http.Client{}
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		fmt.Printf("Error creating Databricks service principal listing request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", dbxToken))

	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error with Databricks service principal listing response: %v\n", err)
		os.Exit(1)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("Error parsing Databricks service principal list: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(body))

	var data struct {
		Resources []ServicePrincipal `json:"Resources"`
	}
	err = json.Unmarshal(body, &data)
	if err != nil {
		log.Fatal(err)
	}

	for _, sp := range data.Resources {
		if strings.ToLower(sp.Name) == strings.ToLower(servicePrincipalName) {
			return true
		}
	}
	return false
}
