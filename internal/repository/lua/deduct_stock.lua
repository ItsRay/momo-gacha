-- KEYS[1]: gacha:prize:{prize_id}:stock
-- ARGV[1]: delta (amount to deduct, default 1)
-- Returns:
--  1: Success (stock decremented)
-- -1: Key does not exist
-- -2: Out of stock
local key = KEYS[1]
local delta = tonumber(ARGV[1] or 1)
local current = redis.call('get', key)

if not current then
    return -1
end

local stock = tonumber(current)
if stock < delta then
    return -2
end

redis.call('decrby', key, delta)
return 1
