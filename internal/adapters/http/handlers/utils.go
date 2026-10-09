package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"gopkg.in/yaml.v3"
)

// decodeRequestBody decodes the request body as JSON or YAML based on Content-Type header
func decodeRequestBody(r *http.Request, v interface{}) error {
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "yaml") || strings.Contains(contentType, "yml") {
		return yaml.NewDecoder(r.Body).Decode(v)
	}
	return json.NewDecoder(r.Body).Decode(v)
}
