package handler

import (
	"net/http"
)

type GachaHandler struct {
	// TODO: Inject DrawGachaUsecase
}

func NewGachaHandler() *GachaHandler {
	return &GachaHandler{}
}

func (h *GachaHandler) Draw(w http.ResponseWriter, r *http.Request) {
	// TODO: POST /v1/campaigns/{id}/draw
	// Get X-User-Id from headers
	// Call DrawGachaUsecase
}
