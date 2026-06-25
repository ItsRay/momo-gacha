-- KEYS[1]: campaign:{campaign_id}:prize:{prize_id}:stock
-- Returns:
--  1: Success (stock decremented)
-- -1: Key does not exist
-- -2: Out of stock
local key = KEYS[1]
local current = redis.call('get', key)

if not current then
    return -1
end

local stock = tonumber(current)
if stock <= 0 then
    return -2
end

redis.call('decr', key)
return 1
