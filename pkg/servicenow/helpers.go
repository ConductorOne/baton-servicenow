package servicenow

import (
	"strconv"
	"strings"
	"text/template"
)

type URLParams map[string]string

func GenerateURL(baseURL string, params URLParams) (string, error) {
	tmpl, err := template.New("url").Parse(baseURL)
	if err != nil {
		return "", err
	}
	var urlBuilder strings.Builder
	err = tmpl.Execute(&urlBuilder, params)
	if err != nil {
		return "", err
	}
	return urlBuilder.String(), nil
}

func ConvertPageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	return strconv.Atoi(token)
}
