package handler

import (
	"encoding/json"
	"net/http"

	"momo-gacha/internal/domain"
	"momo-gacha/internal/usecase"
	"momo-gacha/pkg/response"
)

type AdminHandler struct {
	campaignUC usecase.CampaignUsecase
}

func NewAdminHandler(campaignUC usecase.CampaignUsecase) *AdminHandler {
	return &AdminHandler{
		campaignUC: campaignUC,
	}
}

func (h *AdminHandler) CreateCampaign(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		response.Error(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}

	var campaign domain.Campaign
	if err := json.NewDecoder(r.Body).Decode(&campaign); err != nil {
		response.Error(w, http.StatusBadRequest, 400, "invalid request body")
		return
	}

	err := h.campaignUC.CreateCampaign(r.Context(), &campaign)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, 500, err.Error())
		return
	}

	response.JSON(w, http.StatusCreated, 200, "campaign created successfully", campaign)
}

func (h *AdminHandler) UpdatePrizeWeights(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		response.Error(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}

	campaignID := r.PathValue("id")
	if campaignID == "" {
		response.Error(w, http.StatusBadRequest, 400, "missing campaign id")
		return
	}

	var prizes []domain.Prize
	if err := json.NewDecoder(r.Body).Decode(&prizes); err != nil {
		response.Error(w, http.StatusBadRequest, 400, "invalid request body")
		return
	}

	err := h.campaignUC.UpdatePrizeWeights(r.Context(), campaignID, prizes)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, 500, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, 200, "prize weights updated successfully", nil)
}

func (h *AdminHandler) GetCampaignStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		response.Error(w, http.StatusMethodNotAllowed, 405, "method not allowed")
		return
	}

	campaignID := r.PathValue("id")
	if campaignID == "" {
		response.Error(w, http.StatusBadRequest, 400, "missing campaign id")
		return
	}

	campaign, err := h.campaignUC.GetCampaignStats(r.Context(), campaignID)
	if err != nil {
		response.Error(w, http.StatusInternalServerError, 500, err.Error())
		return
	}

	response.JSON(w, http.StatusOK, 200, "success", campaign)
}
