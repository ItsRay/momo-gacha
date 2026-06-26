package redis

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"momo-gacha/internal/domain"
	"momo-gacha/internal/repository/lua"
	"momo-gacha/pkg/logger"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var deductStockScript = lua.DeductStockScript

type GachaRepository struct {
	db       *sql.DB
	rdb      *redis.Client
	sfGroup  *singleflight.Group
	luaSHA   string
}

// NewGachaRepository creates a new GachaRepository instance.
func NewGachaRepository(db *sql.DB, rdb *redis.Client) *GachaRepository {
	return &GachaRepository{
		db:      db,
		rdb:     rdb,
		sfGroup: &singleflight.Group{},
	}
}

// cache keys
func campaignCacheKey(campaignID string) string {
	return fmt.Sprintf("gacha:campaign:%s:config", campaignID)
}

func prizeStockKey(prizeID string) string {
	return fmt.Sprintf("gacha:prize:%s:stock", prizeID)
}

func (r *GachaRepository) CreateCampaign(ctx context.Context, campaign *domain.Campaign) error {
	// Start DB transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Insert Campaign into MySQL
	_, err = tx.ExecContext(ctx,
		"INSERT INTO gacha_campaigns (id, name, status) VALUES (?, ?, ?)",
		campaign.ID, campaign.Name, campaign.Status,
	)
	if err != nil {
		return fmt.Errorf("mysql insert campaign failed: %w", err)
	}

	// 2. Insert Prizes into MySQL
	for _, prize := range campaign.Prizes {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO gacha_prizes (id, gacha_campaign_id, type, name, prob_bps, init_stock, remained_stock) VALUES (?, ?, ?, ?, ?, ?, ?)",
			prize.ID, campaign.ID, prize.Type, prize.Name, prize.ProbBps, prize.InitStock, prize.InitStock,
		)
		if err != nil {
			return fmt.Errorf("mysql insert prize failed: %w", err)
		}
	}

	// Commit MySQL transaction
	if err := tx.Commit(); err != nil {
		return err
	}

	// 3. Initialize Redis stock for limited prizes (Soft failure: log but do not fail the request)
	for _, prize := range campaign.Prizes {
		if prize.Type == domain.PrizeLimited {
			if serr := r.rdb.Set(ctx, prizeStockKey(prize.ID), prize.InitStock, 0).Err(); serr != nil {
				logger.Warn("Redis stock init failed for prize %s: %v. Relying on cache hydration.", prize.ID, serr)
			}
		}
	}

	// Cache campaign config instantly to Redis (Best effort)
	r.cacheCampaign(ctx, campaign)

	return nil
}

func (r *GachaRepository) GetCampaign(ctx context.Context, id string) (*domain.Campaign, error) {
	// 1. Read from Redis Cache
	val, err := r.rdb.Get(ctx, campaignCacheKey(id)).Result()
	if err == nil {
		var campaign domain.Campaign
		if err := json.Unmarshal([]byte(val), &campaign); err == nil {
			return &campaign, nil
		}
	}

	// 2. Cache Miss: Use Singleflight to read DB
	v, err, _ := r.sfGroup.Do(id, func() (interface{}, error) {
		// Double check cache inside singleflight
		val, err := r.rdb.Get(ctx, campaignCacheKey(id)).Result()
		if err == nil {
			var campaign domain.Campaign
			if err := json.Unmarshal([]byte(val), &campaign); err == nil {
				return &campaign, nil
			}
		}

		// Query DB
		campaign, err := r.getCampaignFromDB(ctx, id)
		if err != nil {
			return nil, err
		}
		if campaign == nil {
			return nil, nil
		}

		// Rebuild stock cache for limited prizes if they are missing in Redis (Cache Hydration)
		for _, prize := range campaign.Prizes {
			if prize.Type == domain.PrizeLimited {
				// Check if stock key exists in Redis
				exists, err := r.rdb.Exists(ctx, prizeStockKey(prize.ID)).Result()
				if err == nil && exists == 0 {
					// If missing, initialize Redis stock from DB remained_stock
					if err := r.rdb.Set(ctx, prizeStockKey(prize.ID), prize.RemainedStock, 0).Err(); err != nil {
						logger.Warn("failed to rebuild Redis stock for prize %s: %v", prize.ID, err)
					}
				}
			}
		}

		// Cache to Redis
		r.cacheCampaign(ctx, campaign)

		return campaign, nil
	})

	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return v.(*domain.Campaign), nil
}

func (r *GachaRepository) UpdatePrizeWeights(ctx context.Context, campaignID string, prizes []domain.Prize) error {
	// Start DB transaction
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Update each prize weight in MySQL
	for _, p := range prizes {
		res, err := tx.ExecContext(ctx,
			"UPDATE gacha_prizes SET prob_bps = ?, updated_at = NOW() WHERE id = ? AND gacha_campaign_id = ?",
			p.ProbBps, p.ID, campaignID,
		)
		if err != nil {
			return fmt.Errorf("update prize weight failed: %w", err)
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			return fmt.Errorf("prize %s not found or no change for campaign %s", p.ID, campaignID)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// 2. Invalidate cache in Redis (Delete)
	if err := r.rdb.Del(ctx, campaignCacheKey(campaignID)).Err(); err != nil {
		logger.Error("failed to invalidate campaign cache for campaign %s: %v", campaignID, err)
	}

	return nil
}

func (r *GachaRepository) DeductStock(ctx context.Context, campaignID, prizeID string, delta int) (int64, error) {
	// Run Lua script to decrement stock atomically
	if r.luaSHA == "" {
		sha, err := r.rdb.ScriptLoad(ctx, deductStockScript).Result()
		if err != nil {
			// Fallback to direct eval if script load fails
			return r.evalDirect(ctx, prizeStockKey(prizeID), delta)
		}
		r.luaSHA = sha
	}

	res, err := r.rdb.EvalSha(ctx, r.luaSHA, []string{prizeStockKey(prizeID)}, delta).Result()
	if err != nil {
		return 0, err
	}

	return res.(int64), nil
}

func (r *GachaRepository) evalDirect(ctx context.Context, key string, delta int) (int64, error) {
	res, err := r.rdb.Eval(ctx, deductStockScript, []string{key}, delta).Result()
	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (r *GachaRepository) GetPrizeStock(ctx context.Context, prizeID string) (int, error) {
	// Try Redis first
	val, err := r.rdb.Get(ctx, prizeStockKey(prizeID)).Result()
	if err == nil {
		if stock, err := strconv.Atoi(val); err == nil {
			return stock, nil
		}
	}

	// Try DB
	var stock int
	err = r.db.QueryRowContext(ctx, "SELECT remained_stock FROM gacha_prizes WHERE id = ?", prizeID).Scan(&stock)
	if err != nil {
		return 0, err
	}

	// Sync back to Redis if found
	if err := r.rdb.Set(ctx, prizeStockKey(prizeID), stock, 0).Err(); err != nil {
		logger.Warn("failed to sync back stock to Redis for prize %s: %v", prizeID, err)
	}

	return stock, nil
}

func (r *GachaRepository) GetCampaignWithLiveStock(ctx context.Context, id string) (*domain.Campaign, error) {
	campaign, err := rctx, id)
	if err != nil {
		return nil, err
	}
	if campaign == nil {
		return nil, nil
	}

	pipe := r.rdb.Pipeline()
	cmds := make(map[string]*redis.StringCmd)

	for _, prize := range campaign.Prizes {
		if prize.Type == domain.PrizeLimited {
			cmds[prize.ID] = pipe.Get(ctx, prizeStockKey(prize.ID))
		}
	}

	if len(cmds) > 0 {
		_, _ = pipe.Exec(ctx)

		for i, prize := range campaign.Prizes {
			if cmd, ok := cmds[prize.ID]; ok {
				val, err := cmd.Result()
				if err == nil {
					if stock, err := strconv.Atoi(val); err == nil {
						campaign.Prizes[i].RemainedStock = stock
					}
				} else {
					var dbStock int
					err = r.db.QueryRowContext(ctx, "SELECT remained_stock FROM gacha_prizes WHERE id = ?", prize.ID).Scan(&dbStock)
					if err == nil {
						campaign.Prizes[i].RemainedStock = dbStock
					}
				}
			}
		}
	}

	return campaign, nil
}

// Private helper to fetch Campaign and its Prizes from DB
func (r *GachaRepository) getCampaignFromDB(ctx context.Context, campaignID string) (*domain.Campaign, error) {
	var campaign domain.Campaign
	err := r.db.QueryRowContext(ctx,
		"SELECT id, name, status, created_at, updated_at FROM gacha_campaigns WHERE id = ?",
		campaignID,
	).Scan(&campaign.ID, &campaign.Name, &campaign.Status, &campaign.CreatedAt, &campaign.UpdatedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	rows, err := r.db.QueryContext(ctx,
		"SELECT id, gacha_campaign_id, type, name, prob_bps, init_stock, remained_stock, created_at, updated_at FROM gacha_prizes WHERE gacha_campaign_id = ?",
		campaignID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var p domain.Prize
		err := rows.Scan(&p.ID, &p.CampaignID, &p.Type, &p.Name, &p.ProbBps, &p.InitStock, &p.RemainedStock, &p.CreatedAt, &p.UpdatedAt)
		if err != nil {
			return nil, err
		}
		campaign.Prizes = append(campaign.Prizes, p)
	}

	return &campaign, nil
}

// Private helper to serialize and cache campaign in Redis
func (r *GachaRepository) cacheCampaign(ctx context.Context, campaign *domain.Campaign) {
	data, err := json.Marshal(campaign)
	if err == nil {
		if err := r.rdb.Set(ctx, campaignCacheKey(campaign.ID), string(data), 30*24*time.Hour).Err(); err != nil {
			logger.Warn("failed to cache campaign config for campaign %s: %v", campaign.ID, err)
		}
	}
}

func (r *GachaRepository) RollbackStock(ctx context.Context, campaignID, prizeID string, delta int) error {
	return r.rdb.IncrBy(ctx, prizeStockKey(prizeID), int64(delta)).Err()
}
