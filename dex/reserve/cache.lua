local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("ReserveCache")
local Utils = require('dex.utils.utils')

local Cache = {}

-- Initialize the cache
function Cache.init()
  Cache.data = {}
  Cache.accessTimes = {}
  Cache.size = 0
  Cache.maxSize = Constants.OPTIMIZATION.CACHE_SIZE
  Cache.expiry = Constants.TIME.RESERVE_CACHE_EXPIRY

  Logger.info("Reserve cache initialized", { maxSize = Cache.maxSize, expiry = Cache.expiry })
  return Cache
end

-- Set a cache item
function Cache.set(poolId, reserves)
  if not poolId or not reserves then
    return false, "Pool ID and reserves are required"
  end

  -- If cache is full and this is a new entry, remove oldest item
  if Cache.size >= Cache.maxSize and not Cache.data[poolId] then
    Cache.removeOldest()
  end

  -- Add or update entry
  local currentTime = os.time()

  Cache.data[poolId] = {
    reserves = reserves,
    timestamp = currentTime
  }

  Cache.accessTimes[poolId] = currentTime

  -- Increment size only for new entries
  if not Cache.data[poolId] then
    Cache.size = Cache.size + 1
  end

  Logger.debug("Cache item set", { pool = poolId })
  return true
end

-- Get a cache item
function Cache.get(poolId)
  if not poolId or not Cache.data[poolId] then
    return nil
  end

  local cacheItem = Cache.data[poolId]
  local currentTime = os.time()

  -- Check if item is expired
  if (currentTime - cacheItem.timestamp) > Cache.expiry then
    Cache.remove(poolId)
    return nil
  end

  -- Update access time for LRU tracking
  Cache.accessTimes[poolId] = currentTime

  Logger.debug("Cache hit", { pool = poolId })
  return cacheItem.reserves
end

-- Check if a cache item exists and is fresh
function Cache.isFresh(poolId)
  if not poolId or not Cache.data[poolId] then
    return false
  end

  local cacheItem = Cache.data[poolId]
  local currentTime = os.time()

  return (currentTime - cacheItem.timestamp) <= Cache.expiry
end

-- Remove a cache item
function Cache.remove(poolId)
  if not poolId or not Cache.data[poolId] then
    return false
  end

  Cache.data[poolId] = nil
  Cache.accessTimes[poolId] = nil
  Cache.size = Cache.size - 1

  Logger.debug("Cache item removed", { pool = poolId })
  return true
end

-- Remove the oldest accessed cache item (for LRU eviction)
function Cache.removeOldest()
  local oldestTime = math.huge
  local oldestKey = nil

  for poolId, accessTime in pairs(Cache.accessTimes) do
    if accessTime < oldestTime then
      oldestTime = accessTime
      oldestKey = poolId
    end
  end

  if oldestKey then
    Cache.remove(oldestKey)
    Logger.debug("Removed oldest cache item", { pool = oldestKey, lastAccessed = oldestTime })
    return true
  end

  return false
end

-- Get all cached items
function Cache.getAll()
  return Cache.data
end

-- Clear all expired items from cache
function Cache.clearExpired()
  local currentTime = os.time()
  local removedCount = 0

  for poolId, cacheItem in pairs(Cache.data) do
    if (currentTime - cacheItem.timestamp) > Cache.expiry then
      Cache.remove(poolId)
      removedCount = removedCount + 1
    end
  end

  Logger.debug("Cleared expired cache items", { removed = removedCount, remaining = Cache.size })
  return removedCount
end

-- Clear the entire cache
function Cache.clear()
  Cache.data = {}
  Cache.accessTimes = {}
  Cache.size = 0

  Logger.info("Cache cleared")
  return true
end

-- Get cache statistics
function Cache.getStats()
  local currentTime = os.time()
  local expiredCount = 0
  local freshCount = 0

  for poolId, cacheItem in pairs(Cache.data) do
    if (currentTime - cacheItem.timestamp) > Cache.expiry then
      expiredCount = expiredCount + 1
    else
      freshCount = freshCount + 1
    end
  end

  return {
    size = Cache.size,
    maxSize = Cache.maxSize,
    fresh = freshCount,
    expired = expiredCount,
    hitRate = Cache.hitRate,
    missRate = Cache.missRate
  }
end

-- Update cache configuration
function Cache.configure(options)
  if options.maxSize then
    Cache.maxSize = options.maxSize
  end

  if options.expiry then
    Cache.expiry = options.expiry
  end

  Logger.info("Cache configuration updated", { maxSize = Cache.maxSize, expiry = Cache.expiry })
  return true
end

-- Track cache hit/miss statistics
Cache.hits = 0
Cache.misses = 0
Cache.hitRate = 0
Cache.missRate = 0

-- Get with hit/miss tracking
function Cache.getWithStats(poolId)
  local result = Cache.get(poolId)

  if result then
    Cache.hits = Cache.hits + 1
  else
    Cache.misses = Cache.misses + 1
  end

  local total = Cache.hits + Cache.misses
  if total > 0 then
    Cache.hitRate = Cache.hits / total
    Cache.missRate = Cache.misses / total
  end

  return result
end

-- Reset hit/miss statistics
function Cache.resetStats()
  Cache.hits = 0
  Cache.misses = 0
  Cache.hitRate = 0
  Cache.missRate = 0

  Logger.debug("Cache statistics reset")
  return true
end

return Cache
