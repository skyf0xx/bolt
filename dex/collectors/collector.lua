local Constants = require('dex.utils.constants')
local Logger = require('dex.utils.logger')
Logger = Logger.createLogger("Collector")
local Utils = require('dex.utils.utils')
local Permaswap = require('dex.collectors.permaswap')
local Botega = require('dex.collectors.botega')
local TokenRepository = require('dex.db.token_repository')
local PoolRepository = require('dex.db.pool_repository')

local Collector = {}

-- Initialize the collector with database connection
function Collector.init(db)
  Collector.db = db
  Collector.pendingCollections = {} -- Track pending collections with timestamps
  Logger.info("Collector initialized")
  return Collector
end

-- Collect data from a specific DEX
function Collector.collectFromDex(source, poolAddresses, callback)
  if not poolAddresses or #poolAddresses == 0 then
    callback({
      pools = {},
      tokens = {},
      reserves = {},
      errors = { message = "No pool addresses provided" }
    })
    return
  end

  local collector
  if source == Constants.SOURCE.PERMASWAP then
    collector = Permaswap
  elseif source == Constants.SOURCE.BOTEGA then
    collector = Botega
  else
    callback({
      pools = {},
      tokens = {},
      reserves = {},
      errors = { message = "Invalid source: " .. tostring(source) }
    })
    return
  end

  -- Create a unique ID for this collection operation
  local operationId = source .. "-" .. os.time() .. "-" .. math.random(1000, 9999)

  -- Track this operation
  Collector.pendingCollections[operationId] = {
    source = source,
    startTime = os.time(),
    poolCount = #poolAddresses,
    completedPools = 0,
    callback = callback
  }

  Logger.info("Collecting data from " .. source, { poolCount = #poolAddresses, operationId = operationId })
  collector.collectAllPoolsData(poolAddresses, function(results)
    -- Remove from pending operations
    Collector.pendingCollections[operationId] = nil
    Logger.info("Collection completed for " .. source, { operationId = operationId })
    callback(results)
  end)
end

-- Collect data from all configured DEXes
function Collector.collectAll(poolList, finalCallback)
  local results = {
    pools = {},
    tokens = {},
    reserves = {},
    errors = {}
  }

  -- If poolList is provided, use it; otherwise, load from database or use default
  if not poolList then
    poolList = Collector.getConfiguredPools()
  end

  -- Organize pools by source
  local poolsBySource = {}
  for _, pool in ipairs(poolList) do
    if not pool.source or not pool.address then
      table.insert(results.errors, { message = "Invalid pool configuration", pool = pool })
      goto continue
    end

    poolsBySource[pool.source] = poolsBySource[pool.source] or {}
    table.insert(poolsBySource[pool.source], pool.address)

    ::continue::
  end

  -- Track pending sources
  local pendingSources = 0
  for _ in pairs(poolsBySource) do
    pendingSources = pendingSources + 1
  end

  if pendingSources == 0 then
    -- No valid sources, return empty results
    finalCallback(results)
    return
  end

  -- Create a unique ID for this combined collection operation
  local operationId = "all-" .. os.time() .. "-" .. math.random(1000, 9999)

  -- Track this operation
  Collector.pendingCollections[operationId] = {
    source = "multiple",
    startTime = os.time(),
    poolCount = Utils.tableSize(poolsBySource),
    completedSources = 0,
    callback = finalCallback
  }

  -- Collect from each source
  for source, addresses in pairs(poolsBySource) do
    Collector.collectFromDex(source, addresses, function(sourceResults)
      pendingSources = pendingSources - 1

      -- Update tracking
      if Collector.pendingCollections[operationId] then
        Collector.pendingCollections[operationId].completedSources =
            Collector.pendingCollections[operationId].completedSources + 1
      end

      -- Merge results
      for _, pool in ipairs(sourceResults.pools) do
        table.insert(results.pools, pool)
      end

      for _, token in ipairs(sourceResults.tokens) do
        local exists = false
        for i, existingToken in ipairs(results.tokens) do
          if existingToken.id == token.id then
            -- Update token if new data is more complete
            if token.symbol and token.symbol ~= "" and existingToken.symbol == "" then
              results.tokens[i].symbol = token.symbol
            end
            if token.name and token.name ~= "" and existingToken.name == "" then
              results.tokens[i].name = token.name
            end
            exists = true
            break
          end
        end

        if not exists then
          table.insert(results.tokens, token)
        end
      end

      -- Merge reserves
      for poolId, reserve in pairs(sourceResults.reserves) do
        results.reserves[poolId] = reserve
      end

      -- Merge errors
      for poolId, err in pairs(sourceResults.errors) do
        results.errors[poolId] = err
      end

      -- If all sources are processed, return the results
      if pendingSources == 0 then
        -- Remove from pending operations
        Collector.pendingCollections[operationId] = nil

        Logger.info("Collection completed", {
          poolCount = #results.pools,
          tokenCount = #results.tokens,
          reserveCount = Utils.tableSize(results.reserves),
          errorCount = Utils.tableSize(results.errors)
        })

        finalCallback(results)
      end
    end)
  end
end

-- Save collected data to database
function Collector.saveToDatabase(data, callback)
  if not Collector.db then
    callback(false, "Database not initialized")
    return
  end

  local db = Collector.db
  Logger.info("Saving collected data to database")

  -- Begin transaction
  db:exec("BEGIN TRANSACTION")

  -- Save tokens
  local tokenSuccess, tokenErr = TokenRepository.batchInsertTokens(db, data.tokens)
  if not tokenSuccess then
    db:exec("ROLLBACK")
    Logger.error("Failed to save tokens", { error = tokenErr })
    callback(false, "Failed to save tokens: " .. tokenErr)
    return
  end

  -- Save pools
  local poolSuccess, poolErr = PoolRepository.batchInsertPools(db, data.pools)
  if not poolSuccess then
    db:exec("ROLLBACK")
    Logger.error("Failed to save pools", { error = poolErr })
    callback(false, "Failed to save pools: " .. poolErr)
    return
  end

  -- Track pending reserve updates
  local pendingReserves = Utils.tableSize(data.reserves)

  if pendingReserves == 0 then
    -- No reserves to update, commit and return
    db:exec("COMMIT")
    Logger.info("Data saved to database", {
      tokens = #data.tokens,
      pools = #data.pools,
      reserves = 0
    })
    callback(true)
    return
  end

  -- Save reserves
  for poolId, reserve in pairs(data.reserves) do
    local reserveA = reserve.reserve_a or "0"
    local reserveB = reserve.reserve_b or "0"

    local reserveSuccess, reserveErr = PoolRepository.updateReserves(db, poolId, reserveA, reserveB)
    pendingReserves = pendingReserves - 1

    if not reserveSuccess then
      Logger.warn("Failed to update reserves for pool", { pool = poolId, error = reserveErr })
      -- Continue with other reserves
    end

    -- If all reserves are processed, commit and return
    if pendingReserves == 0 then
      db:exec("COMMIT")
      Logger.info("Data saved to database", {
        tokens = #data.tokens,
        pools = #data.pools,
        reserves = Utils.tableSize(data.reserves)
      })
      callback(true)
    end
  end
end

-- Get pools that need reserve refresh
function Collector.getPoolsNeedingRefresh(maxAge, callback)
  if not Collector.db then
    callback({}, "Database not initialized")
    return
  end

  local pools = PoolRepository.getPoolsNeedingReserveRefresh(Collector.db, maxAge)
  callback(pools)
end

-- Get configured pool list (either from database or configuration)
function Collector.getConfiguredPools()
  -- Try to load from database first
  if Collector.db then
    local pools = PoolRepository.getAllPools(Collector.db)
    if pools and #pools > 0 then
      local configuredPools = {}
      for _, pool in ipairs(pools) do
        table.insert(configuredPools, {
          address = pool.id,
          source = pool.source
        })
      end
      return configuredPools
    end
  end

  -- If no pools in database, return an empty list
  -- In a real implementation, this might load from a config file
  Logger.warn("No pools found in database and no default configuration provided")
  return {}
end

-- Calculate output for a specific path (used by path finder)
function Collector.calculatePathOutput(path, inputAmount, callback)
  local result = {
    inputAmount = inputAmount,
    amount_out = inputAmount,
    steps = {}
  }

  -- Process path steps recursively
  local function processStep(index, currentInput)
    if index > #path then
      -- All steps processed, return result
      result.amount_out = currentInput
      callback(result)
      return
    end

    local step = path[index]
    local poolId = step.pool_id
    local tokenIn = step.from
    local tokenOut = step.to

    -- Create a unique ID for this step's calculation
    local operationId = "path-step-" .. poolId .. "-" .. os.time() .. "-" .. math.random(1000, 9999)

    -- Track this operation
    Collector.pendingCollections[operationId] = {
      source = "path-step",
      startTime = os.time(),
      poolId = poolId,
      tokenIn = tokenIn,
      tokenOut = tokenOut,
      callback = callback -- store original callback for potential flush
    }

    -- Get pool data
    local pool = PoolRepository.getPool(Collector.db, poolId)
    if not pool then
      Collector.pendingCollections[operationId] = nil
      callback(nil, "Pool not found: " .. poolId)
      return
    end

    -- Calculate output amount based on pool source
    if pool.source == Constants.SOURCE.PERMASWAP then
      -- Use Permaswap API instead of formula
      Permaswap.requestOrder(poolId, tokenIn, tokenOut, currentInput, function(response, err)
        -- Remove from pending operations
        Collector.pendingCollections[operationId] = nil

        if err or not response then
          callback(nil, err or "Failed to get output amount from Permaswap")
          return
        end

        local amount_out = response.amount_out

        -- Add step to result
        table.insert(result.steps, {
          pool_id = poolId,
          source = pool.source,
          token_in = tokenIn,
          token_out = tokenOut,
          amount_in = currentInput,
          amount_out = amount_out,
          fee_bps = pool.fee_bps
        })

        -- Process next step
        processStep(index + 1, amount_out)
      end)
    elseif pool.source == Constants.SOURCE.BOTEGA then
      -- Use Botega API instead of formula
      Botega.getSwapOutput(poolId, tokenIn, currentInput, nil, function(response, err)
        -- Remove from pending operations
        Collector.pendingCollections[operationId] = nil

        if err or not response then
          callback(nil, err or "Failed to get swap output from Botega")
          return
        end

        local amount_out = response.amount_out

        -- Add step to result
        table.insert(result.steps, {
          pool_id = poolId,
          source = pool.source,
          token_in = tokenIn,
          token_out = tokenOut,
          amount_in = currentInput,
          amount_out = amount_out,
          fee_bps = pool.fee_bps
        })

        -- Process next step
        processStep(index + 1, amount_out)
      end)
    else
      Collector.pendingCollections[operationId] = nil
      callback(nil, "Unsupported pool source: " .. tostring(pool.source))
    end
  end

  -- Start processing from the first step
  processStep(1, inputAmount)
end

-- Execute a swap across multiple pools (path)
function Collector.executePathSwap(path, inputAmount, minOutputAmount, userAddress, callback)
  -- This would execute a swap across multiple pools
  -- In a production implementation, this would need to handle:
  -- 1. Cross-DEX token transfers
  -- 2. Atomic execution (or as close as possible)
  -- 3. Slippage protection at each step

  Logger.warn("Path execution not fully implemented, would execute multiple swaps")
  callback(nil, "Multi-pool swaps require cross-process coordination")
end

-- Flush pending collections that have been running for too long
function Collector.flushPendingCollections(maxAge, forced)
  maxAge = maxAge or 60 -- Default to 60 seconds
  local flushedCount = 0
  local currentTime = os.time()

  -- Create a list of operations to flush
  local toFlush = {}

  for id, operation in pairs(Collector.pendingCollections) do
    if forced or (currentTime - operation.startTime) > maxAge then
      table.insert(toFlush, id)
    end
  end

  -- Flush each pending operation
  for _, id in ipairs(toFlush) do
    local operation = Collector.pendingCollections[id]
    if operation then
      Logger.warn("Flushing stalled collection", {
        id = id,
        source = operation.source,
        pendingFor = currentTime - operation.startTime,
        poolId = operation.poolId,
        poolCount = operation.poolCount,
        completedPools = operation.completedPools or 0
      })

      -- Create a partial result to return
      local partialResults = {
        pools = {},
        tokens = {},
        reserves = {},
        errors = {
          message = "Collection timed out or was manually flushed",
          timeElapsed = currentTime - operation.startTime,
          operationId = id
        }
      }

      -- Call the original callback with partial results
      if operation.callback then
        operation.callback(partialResults)
      end

      -- Remove from pending list
      Collector.pendingCollections[id] = nil
      flushedCount = flushedCount + 1
    end
  end

  return {
    flushedCount = flushedCount,
    remainingCount = Utils.tableSize(Collector.pendingCollections),
    pendingOperations = Collector.getPendingOperationsSummary()
  }
end

-- Get a list of pending operations for diagnostic purposes
function Collector.getPendingOperationsSummary()
  local summary = {}
  local currentTime = os.time()

  for id, operation in pairs(Collector.pendingCollections) do
    table.insert(summary, {
      id = id,
      source = operation.source,
      runningTime = currentTime - operation.startTime,
      poolCount = operation.poolCount,
      completedPools = operation.completedPools or 0,
      completedSources = operation.completedSources or 0
    })
  end

  return summary
end

return Collector
