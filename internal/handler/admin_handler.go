package handler

import (
	"net/http"
)

type AdminHandler struct {
	// TODO: Inject update prize usecase and create campaign usecase
}

func NewAdminHandler() *AdminHandler {
	return &AdminHandler{}
}

func (h *AdminHandler) CreateCampaign(w http.ResponseWriter, r *http.Request) {
	// TODO: POST /v1/admin/campaigns
}

func (h *AdminHandler) UpdatePrizeWeights(w http.ResponseWriter, r *http.Request) {
	// TODO: PUT /v1/admin/campaigns/{id}/prizes
}

func (h *AdminHandler) GetCampaignStats(w http.ResponseWriter, r *http.Request) {
	// TODO: GET /v1/admin/campaigns/{id}/stats
}
