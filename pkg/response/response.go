package response

import (
	"encoding/json"
	"net/http"
)

type JSONResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// JSON sends a JSON response with the given status code.
func JSON(w http.ResponseWriter, statusCode int, code int, msg string, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(JSONResponse{
		Code:    code,
		Message: msg,
		Data:    data,
	})
}

// Error sends an error response with the given status code.
func Error(w http.ResponseWriter, statusCode int, code int, msg string) {
	JSON(w, statusCode, code, msg, nil)
}
