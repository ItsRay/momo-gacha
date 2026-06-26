package handler

import (
	"errors"
	"net/http"

	"momo-gacha/internal/domain"
	"momo-gacha/internal/usecase"
	"momo-gacha/pkg/response"
)

type GachaHandler struct {
	drawGachaUC usecase.DrawGachaUsecase
}

func NewGachaHandler(drawGachaUC usecase.DrawGachaUsecase) *GachaHandler {
	return &GachaHandler{
		drawGachaUC: drawGachaUC,
	}
}

func (h *GachaHandler) Draw(w http.ResponseWriter, r *http.Request) {
	campaignID := r.PathValue("id")
	if campaignID == "" {
		response.Error(w, http.StatusBadRequest, 400, "missing campaign id")
		return
	}

	userID := r.Header.Get("X-User-Id")
	if userID == "" {
		response.Error(w, http.StatusUnauthorized, 401, "missing X-User-Id header")
		return
	}

	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		response.Error(w, http.StatusBadRequest, 400, "missing Idempotency-Key header")
		return
	}

	prize, err := h.drawGachaUC.Draw(r.Context(), campaignID, userID, idempotencyKey)
	if err != nil {
		var conflictErr *domain.ConflictError
		if errors.As(err, &conflictErr) {
			response.Error(w, http.StatusConflict, 409, conflictErr.Error())
			return
		}
		var bizErr *domain.ValidationError
		if errors.As(err, &bizErr) {
			response.Error(w, http.StatusBadRequest, 400, bizErr.Error())
			return
		}
		response.Error(w, http.StatusInternalServerError, 500, "internal server error")
		return
	}

	response.JSON(w, http.StatusOK, 200, "success", prize)
}
