package usecase

import (
	"context"
	"errors"
	"momo-gacha/internal/domain"
	"testing"
)

type mockCampaignUsecaseRepo struct {
	campaigns map[string]*domain.Campaign
}

func (m *mockCampaignUsecaseRepo) CreateCampaign(ctx context.Context, campaign *domain.Campaign) error {
	m.campaigns[campaign.ID] = campaign
	return nil
}

func (m *mockCampaignUsecaseRepo) GetCampaign(ctx context.Context, id string) (*domain.Campaign, error) {
	c, ok := m.campaigns[id]
	if !ok {
		return nil, nil
	}
	return c, nil
}

func (m *mockCampaignUsecaseRepo) GetCampaignWithLiveStock(ctx context.Context, id string) (*domain.Campaign, error) {
	c, ok := m.campaigns[id]
	if !ok {
		return nil, nil
	}
	return c, nil
}

func (m *mockCampaignUsecaseRepo) UpdatePrizeWeights(ctx context.Context, campaignID string, prizes []domain.Prize) error {
	c, ok := m.campaigns[campaignID]
	if !ok {
		return errors.New("not found")
	}
	c.Prizes = prizes
	return nil
}

func (m *mockCampaignUsecaseRepo) DeductStock(ctx context.Context, campaignID, prizeID string, delta int) (int64, error) {
	return 0, nil
}

func (m *mockCampaignUsecaseRepo) GetPrizeStock(ctx context.Context, prizeID string) (int, error) {
	return 0, nil
}

func (m *mockCampaignUsecaseRepo) RollbackStock(ctx context.Context, campaignID, prizeID string, delta int) error {
	return nil
}

func TestCreateCampaign(t *testing.T) {
	repo := &mockCampaignUsecaseRepo{
		campaigns: make(map[string]*domain.Campaign),
	}
	uc := NewCampaignUsecase(repo)

	// Case 1: Valid Campaign
	validCampaign := &domain.Campaign{
		ID:     "cam_1",
		Name:   "New Year Gacha",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "p1", Name: "iPhone 17", Type: domain.PrizeLimited, ProbBps: 1000},
			{ID: "p2", Name: "100 momo coin", Type: domain.PrizeLimited, ProbBps: 2000},
			{ID: "p3", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0},
		},
	}
	err := uc.CreateCampaign(context.Background(), validCampaign)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Case 2: Validation Error - Empty Name
	emptyNameCampaign := &domain.Campaign{
		ID:     "cam_2",
		Name:   "",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "p1", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0},
		},
	}
	err = uc.CreateCampaign(context.Background(), emptyNameCampaign)
	var valErr *domain.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %v", err)
	}

	// Case 3: Validation Error - No Prizes
	noPrizesCampaign := &domain.Campaign{
		ID:     "cam_3",
		Name:   "No Prizes",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{},
	}
	err = uc.CreateCampaign(context.Background(), noPrizesCampaign)
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %v", err)
	}

	// Case 4: Validation Error - Missing Fallback Prize
	noFallbackCampaign := &domain.Campaign{
		ID:     "cam_4",
		Name:   "No Fallback",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "p1", Name: "iPhone 17", Type: domain.PrizeLimited, ProbBps: 1000},
		},
	}
	err = uc.CreateCampaign(context.Background(), noFallbackCampaign)
	if !errors.As(err, &valErr) || err.Error() != "campaign must configure exactly one fallback prize" {
		t.Fatalf("expected exact fallback ValidationError, got %v", err)
	}

	// Case 5: Validation Error - Multiple Fallback Prizes
	multiFallbackCampaign := &domain.Campaign{
		ID:     "cam_5",
		Name:   "Multi Fallback",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "p1", Name: "銘謝惠顧 1", Type: domain.PrizeFallback, ProbBps: 0},
			{ID: "p2", Name: "銘謝惠顧 2", Type: domain.PrizeFallback, ProbBps: 0},
		},
	}
	err = uc.CreateCampaign(context.Background(), multiFallbackCampaign)
	if !errors.As(err, &valErr) || err.Error() != "campaign must configure exactly one fallback prize" {
		t.Fatalf("expected exact fallback ValidationError, got %v", err)
	}

	// Case 6: Validation Error - Total weight exceeds 10000 bps
	overweightCampaign := &domain.Campaign{
		ID:     "cam_6",
		Name:   "Overweight Gacha",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "p1", Name: "iPhone 17", Type: domain.PrizeLimited, ProbBps: 6000},
			{ID: "p2", Name: "100 momo coin", Type: domain.PrizeLimited, ProbBps: 5000},
			{ID: "p3", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0},
		},
	}
	err = uc.CreateCampaign(context.Background(), overweightCampaign)
	if !errors.As(err, &valErr) || err.Error() != "total weight of prizes cannot exceed 10000 (100%)" {
		t.Fatalf("expected overweight ValidationError, got %v", err)
	}

	// Case 7: Conflict Error - Campaign already exists
	err = uc.CreateCampaign(context.Background(), validCampaign)
	var conflictErr *domain.ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected ConflictError, got %v", err)
	}
}

func TestUpdatePrizeWeights(t *testing.T) {
	repo := &mockCampaignUsecaseRepo{
		campaigns: make(map[string]*domain.Campaign),
	}
	uc := NewCampaignUsecase(repo)

	// Setup initial campaign
	initCampaign := &domain.Campaign{
		ID:     "cam_1",
		Name:   "New Year Gacha",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "p1", Name: "iPhone 17", Type: domain.PrizeLimited, ProbBps: 1000},
			{ID: "p2", Name: "100 momo coin", Type: domain.PrizeLimited, ProbBps: 2000},
			{ID: "p3", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0},
		},
	}
	repo.campaigns["cam_1"] = initCampaign

	// Case 1: Valid weights update
	newPrizes := []domain.Prize{
		{ID: "p1", Name: "iPhone 17", Type: domain.PrizeLimited, ProbBps: 500},
		{ID: "p2", Name: "100 momo coin", Type: domain.PrizeLimited, ProbBps: 1500},
		{ID: "p3", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0},
	}
	err := uc.UpdatePrizeWeights(context.Background(), "cam_1", newPrizes)
	if err != nil {
		t.Fatalf("expected successful update, got %v", err)
	}
	if repo.campaigns["cam_1"].Prizes[0].ProbBps != 500 {
		t.Errorf("expected updated weight of 500, got %d", repo.campaigns["cam_1"].Prizes[0].ProbBps)
	}

	// Case 2: Validation Error - Empty prizes list
	err = uc.UpdatePrizeWeights(context.Background(), "cam_1", []domain.Prize{})
	var valErr *domain.ValidationError
	if !errors.As(err, &valErr) {
		t.Fatalf("expected ValidationError, got %v", err)
	}

	// Case 3: Validation Error - Campaign not found
	err = uc.UpdatePrizeWeights(context.Background(), "cam_non_existent", newPrizes)
	if !errors.As(err, &valErr) || err.Error() != "campaign not found" {
		t.Fatalf("expected campaign not found ValidationError, got %v", err)
	}
}

func TestGetCampaignStats(t *testing.T) {
	repo := &mockCampaignUsecaseRepo{
		campaigns: make(map[string]*domain.Campaign),
	}
	uc := NewCampaignUsecase(repo)

	// Setup campaign
	initCampaign := &domain.Campaign{
		ID:     "cam_1",
		Name:   "New Year Gacha",
		Status: domain.CampaignActive,
		Prizes: []domain.Prize{
			{ID: "p1", Name: "iPhone 17", Type: domain.PrizeLimited, ProbBps: 1000},
			{ID: "p2", Name: "100 momo coin", Type: domain.PrizeLimited, ProbBps: 2000},
			{ID: "p3", Name: "銘謝惠顧", Type: domain.PrizeFallback, ProbBps: 0},
		},
	}
	repo.campaigns["cam_1"] = initCampaign

	// Case 1: Found
	c, err := uc.GetCampaignStats(context.Background(), "cam_1")
	if err != nil {
		t.Fatalf("expected campaign stats, got error %v", err)
	}
	if c.ID != "cam_1" {
		t.Errorf("expected ID cam_1, got %s", c.ID)
	}

	// Case 2: Not found
	_, err = uc.GetCampaignStats(context.Background(), "non_existent")
	var valErr *domain.ValidationError
	if !errors.As(err, &valErr) || err.Error() != "campaign not found" {
		t.Fatalf("expected campaign not found ValidationError, got %v", err)
	}
}
