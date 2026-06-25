package main

import (
	"fmt"
	"net/http"
)

func main() {
	fmt.Println("Starting API Server...")
	// TODO: Implement API server logic
	http.ListenAndServe(":8080", nil)
}
