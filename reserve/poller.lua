local Constants = require('arbitrage.constants')
local Logger = require('arbitrage.logger').createLogger("ReservePoller")
local Utils = require('arbitrage.utils')
local Permaswap = require('arbitrage.collectors.permaswap')
local Botega = require('arbitrage.collectors.botega')
local Cache = require('arbitrage.reserve.cache')
local PoolRepository = require('arbitrage.db.pool_repository')

local Poller = {}

-- Initialize the poller with cache and database
function Poller.init(db)
  Poller.db = db
  Poller.cache = Cache.init()
  Poller.inProgressPolls = {}
  Logger.info("Reserve poller initialized")
  return Poller
end

-- Get reserves for a pool, potentially from cache
function Poller.getReserves(poolId, forceFresh, callback)
  -- Check if reserves are being polled already for this pool
  if Poller.inProgressPolls[poolId] then
    Logger.debug("Poll already in progress for pool", { pool = poolId })
    table.insert(Poller.inProgressPolls[poolId], callback)
    return
  end

  -- Try to get from cache if not forcing fresh data
  if not forceFresh then
    local cachedReserves = Poller.cache.getWithStats(poolId)
    if cachedReserves then
      Logger.debug("Using cached reserves", { pool = poolId })
      callback(cachedReserves)
      return
    end
  end

  -- Need to fetch fresh data
  Logger.debug("Fetching fresh reserves", { pool = poolId })

  -- Get pool information from database
  local pool = PoolRepository.getPool(Poller.db, poolId)
  if not pool then
    Logger.error("Pool not found", { pool = poolId })
    callback(nil, Constants.ERROR.POOL_NOT_FOUND)
    return
  end

  -- Track callbacks for this poll
  Poller.inProgressPolls[poolId] = { callback }

  -- Call the appropriate DEX-specific fetch method
  if pool.source == Constants.SOURCE.PERMASWAP then
    Permaswap.fetchReserves(poolId, Poller.handleReservesResponse(poolId, pool))
  elseif pool.source == Constants.SOURCE.BOTEGA then
    Botega.fetchReserves(poolId, Poller.handleReservesResponse(poolId, pool))
  else
    Logger.error("Unsupported pool source", { pool = poolId, source = pool.source })
    Poller.notifyCallbacks(poolId, nil, "Unsupported pool source: " + pool.source)
  end
end

-- Handle reserve response (factory function to close over poolId and pool)
function Poller.handleReservesResponse(poolId, pool)
  return function(reserves, err)
    if not reserves then
      Logger.error("Failed to fetch reserves", { pool = poolId, error = err })
      Poller.notifyCallbacks(poolId, nil, err or Constants.ERROR.REQUEST_TIMEOUT)
      return
    end

    -- Normalize reserves format
    local normalizedReserves
    if pool.source == Constants.SOURCE.PERMASWAP then
      normalizedReserves = {
        reserve_a = reserves.reserveX,
        reserve_b = reserves.reserveY,
        token_a_id = pool.token_a_id,
        token_b_id = pool.token_b_id,
        last_updated = os.time()
      }
    else
      normalizedReserves = {
        reserve_a = reserves.reserveA,
        reserve_b = reserves.reserveB,
        token_a_id = pool.token_a_id,
        token_b_id = pool.token_b_id,
        last_updated = os.time()
      }
    end

    -- Cache the reserves
    Poller.cache.set(poolId, normalizedReserves)

    -- Update database if enabled
    if Poller.db then
      PoolRepository.updateReserves(
        Poller.db,
        poolId,
        normalizedReserves.reserve_a,
        normalizedReserves.reserve_b
      )
    end

    -- Notify all callbacks waiting for this data
    Poller.notifyCallbacks(poolId, normalizedReserves)
  end
end

-- Notify all callbacks for a particular pool
function Poller.notifyCallbacks(poolId, reserves, error)
  local callbacks = Poller.inProgressPolls[poolId] or {}
  Poller.inProgressPolls[poolId] = nil

  for _, callback in ipairs(callbacks) do
    callback(reserves, error)
  end
end

-- Poll reserves for multiple pools in parallel
function Poller.pollMultiplePools(poolIds, forceFresh, finalCallback)
  if not poolIds or #poolIds == 0 then
    finalCallback({}, "No pools specified")
    return
  end

  local results = {
    reserves = {},
    errors = {}
  }

  local pendingPools = #poolIds

  for _, poolId in ipairs(poolIds) do
    Poller.getReserves(poolId, forceFresh, function(reserves, err)
      pendingPools = pendingPools - 1

      if reserves then
        results.reserves[poolId] = reserves
      else
        results.errors[poolId] = err
      end

      -- If all pools are processed, call the final callback
      if pendingPools == 0 then
        Logger.info("Multiple pool polling completed", {
          poolCount = #poolIds,
          successful = Utils.tableSize(results.reserves),
          failed = Utils.tableSize(results.errors)
        })
        finalCallback(results)
      end
    end)
  end
end

-- Poll reserves for all pools along a specific path
function Poller.pollPathReserves(path, forceFresh, callback)
  if not path or #path == 0 then
    callback({}, "Invalid path")
    return
  end

  -- Extract pool IDs from path
  local poolIds = {}
  for _, step in ipairs(path) do
    table.insert(poolIds, step.pool_id)
  end

  -- Remove duplicates (in case same pool is used multiple times)
  poolIds = Utils.uniqueArray(poolIds)

  -- Poll reserves for all pools in the path
  Poller.pollMultiplePools(poolIds, forceFresh, function(results)
    -- Check if we have all required reserves
    local missingPools = {}

    for _, poolId in ipairs(poolIds) do
      if not results.reserves[poolId] then
        table.insert(missingPools, poolId)
      end
    end

    if #missingPools > 0 then
      callback(results, "Missing reserves for pools: " .. table.concat(missingPools, ", "))
    else
      callback(results)
    end
  end)
end

-- Poll all pools that need a refresh
function Poller.refreshStaleReserves(maxAge, batchSize, callback)
  if not Poller.db then
    callback({}, "Database not initialized")
    return
  end

  maxAge = maxAge or Constants.TIME.RESERVE_CACHE_EXPIRY
  batchSize = batchSize or Constants.OPTIMIZATION.BATCH_SIZE

  -- Get pools that need refresh
  local pools = PoolRepository.getPoolsNeedingReserveRefresh(Poller.db, maxAge)

  -- Limit to batch size
  if #pools > batchSize then
    local limitedPools = {}
    for i = 1, batchSize do
      table.insert(limitedPools, pools[i])
    end
    pools = limitedPools
  end

  if #pools == 0 then
    callback({ refreshed = 0 }, "No stale reserves found")
    return
  end

  -- Extract pool IDs
  local poolIds = {}
  for _, pool in ipairs(pools) do
    table.insert(poolIds, pool.id)
  end

  -- Poll fresh reserves
  Poller.pollMultiplePools(poolIds, true, function(results)
    callback({
      refreshed = Utils.tableSize(results.reserves),
      failed = Utils.tableSize(results.errors),
      results = results
    })
  end)
end

-- Start a background refreshing process
function Poller.startBackgroundRefresh(interval, batchSize)
  interval = interval or Constants.TIME.RESERVE_CACHE_EXPIRY
  batchSize = batchSize or Constants.OPTIMIZATION.BATCH_SIZE

  Logger.info("Starting background reserve refresh", { interval = interval, batchSize = batchSize })

  -- Clear any existing timer
  if Poller.refreshTimer then
    Poller.stopBackgroundRefresh()
  end

  -- Create a new timer
  Poller.refreshTimer = true
  Poller.isRefreshing = false

  -- Define the refresh function
  local function doRefresh()
    -- Avoid overlapping refreshes
    if Poller.isRefreshing then
      Logger.debug("Skipping refresh, previous still in progress")
      return
    end

    Poller.isRefreshing = true

    Poller.refreshStaleReserves(interval, batchSize, function(result)
      Poller.isRefreshing = false

      if result.refreshed > 0 then
        Logger.info("Background refresh completed", {
          refreshed = result.refreshed,
          failed = result.failed
        })
      end

      -- Schedule next refresh if timer is still active
      if Poller.refreshTimer then
        ao.schedule(interval, doRefresh)
      end
    end)
  end

  -- Start the refresh cycle
  ao.schedule(interval, doRefresh)
  return true
end

-- Stop background refreshing
function Poller.stopBackgroundRefresh()
  Logger.info("Stopping background reserve refresh")
  Poller.refreshTimer = nil
  return true
end

-- Get cache statistics
function Poller.getCacheStats()
  return Poller.cache.getStats()
end

-- Clear the cache
function Poller.clearCache()
  return Poller.cache.clear()
end

return Poller
