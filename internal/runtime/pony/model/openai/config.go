package openai

import "net/http"

type Config struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}
