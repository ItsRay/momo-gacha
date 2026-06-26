package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"momo-gacha/internal/domain"
)

type rewardRepository struct {
	db *sql.DB
}

// NewRewardRepository creates a new domain.RewardRepository implementation.
func NewRewardRepository(db *sql.DB) domain.RewardRepository {
	return &rewardRepository{
		db: db,
	}
}

func (r *rewardRepository) ExecuteBatchTransaction(ctx context.Context, records []domain.RewardRecord, stockDeductions map[string]int) error {
	if len(records) == 0 && len(stockDeductions) == 0 {
		return nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Bulk Insert reward records
	if len(records) > 0 {
		// Construct placeholders like (?, ?, ?, ?, NOW()), (?, ?, ?, ?, NOW())
		valueStrings := make([]string, 0, len(records))
		valueArgs := make([]interface{}, 0, len(records)*4)
		for _, rec := range records {
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, rec.ID, rec.CampaignID, rec.UserID, rec.PrizeID, rec.CreatedAt)
		}
		stmt := fmt.Sprintf("INSERT INTO gacha_reward_records (id, gacha_campaign_id, user_id, prize_id, created_at) VALUES %s",
			strings.Join(valueStrings, ","))

		_, err = tx.ExecContext(ctx, stmt, valueArgs...)
		if err != nil {
			return fmt.Errorf("bulk insert reward records failed: %w", err)
		}
	}

	// 2. Optimistic lock deductions for each prize stock
	for prizeID, deductCount := range stockDeductions {
		if deductCount <= 0 {
			continue
		}
		// UPDATE gacha_prizes SET remained_stock = remained_stock - ? WHERE id = ? AND remained_stock >= ?
		res, err := tx.ExecContext(ctx,
			"UPDATE gacha_prizes SET remained_stock = remained_stock - ?, updated_at = NOW() WHERE id = ? AND remained_stock >= ?",
			deductCount, prizeID, deductCount,
		)
		if err != nil {
			return fmt.Errorf("optimistic update stock failed for prize %s: %w", prizeID, err)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			// This means database stock was insufficient to cover the deductCount!
			return fmt.Errorf("insufficient stock for prize %s during optimistic lock check", prizeID)
		}
	}

	return tx.Commit()
}

func (r *rewardRepository) InsertRewardRecord(ctx context.Context, record *domain.RewardRecord) error {
	_, err := r.db.ExecContext(ctx,
		"INSERT INTO gacha_reward_records (id, gacha_campaign_id, user_id, prize_id, created_at) VALUES (?, ?, ?, ?, ?)",
		record.ID, record.CampaignID, record.UserID, record.PrizeID, record.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert reward record failed: %w", err)
	}
	return nil
}

func (r *rewardRepository) DeductPrizeStock(ctx context.Context, prizeID string, deductCount int) error {
	res, err := r.db.ExecContext(ctx,
		"UPDATE gacha_prizes SET remained_stock = remained_stock - ?, updated_at = NOW() WHERE id = ? AND remained_stock >= ?",
		deductCount, prizeID, deductCount,
	)
	if err != nil {
		return fmt.Errorf("deduct prize stock failed: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("insufficient stock or prize %s not found in database", prizeID)
	}

	return nil
}

func (r *rewardRepository) ExecuteSingleTransaction(ctx context.Context, record domain.RewardRecord) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Deduct prize stock in DB
	res, err := tx.ExecContext(ctx,
		"UPDATE gacha_prizes SET remained_stock = remained_stock - 1, updated_at = NOW() WHERE id = ? AND remained_stock >= 1",
		record.PrizeID,
	)
	if err != nil {
		return fmt.Errorf("deduct prize stock failed: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("insufficient stock for prize %s", record.PrizeID)
	}

	// 2. Insert reward record
	_, err = tx.ExecContext(ctx,
		"INSERT INTO gacha_reward_records (id, gacha_campaign_id, user_id, prize_id, created_at) VALUES (?, ?, ?, ?, ?)",
		record.ID, record.CampaignID, record.UserID, record.PrizeID, record.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert reward record failed: %w", err)
	}

	return tx.Commit()
}
