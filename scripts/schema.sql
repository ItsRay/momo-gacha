CREATE DATABASE IF NOT EXISTS `momo_gacha` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `momo_gacha`;

-- 1. Campaign Table
CREATE TABLE IF NOT EXISTS `gacha_campaigns` (
    `id` VARCHAR(64) NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `status` VARCHAR(32) NOT NULL DEFAULT 'draft',
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. Prize Table
CREATE TABLE IF NOT EXISTS `gacha_prizes` (
    `id` VARCHAR(64) NOT NULL,
    `gacha_campaign_id` VARCHAR(64) NOT NULL,
    `type` VARCHAR(32) NOT NULL, -- 'limited' or 'fallback'
    `name` VARCHAR(255) NOT NULL,
    `prob_bps` INT NOT NULL DEFAULT 0, -- probability in basis points (10000 = 100%)
    `init_stock` INT NOT NULL DEFAULT 0,
    `remained_stock` INT NOT NULL DEFAULT 0,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    CONSTRAINT `fk_gacha_prizes_campaign` FOREIGN KEY (`gacha_campaign_id`) REFERENCES `gacha_campaigns` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. Reward Records Table (Ledger/Flow table)
CREATE TABLE IF NOT EXISTS `gacha_reward_records` (
    `id` VARCHAR(64) NOT NULL,
    `gacha_campaign_id` VARCHAR(64) NOT NULL,
    `user_id` VARCHAR(64) NOT NULL,
    `prize_id` VARCHAR(64) NOT NULL,
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `idx_campaign_user` (`gacha_campaign_id`, `user_id`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
